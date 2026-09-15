/// In-memory backend to be used for testing
pub mod in_memory_backend {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::RwLock,
    };

    use bytes::{Bytes, BytesMut};
    use enum_map::EnumMap;

    use rustic_core::{
        BytesList, ErrorKind, FileType, Id, ReadBackend, RusticError, RusticResult, WriteBackend,
    };

    #[derive(Debug)]
    /// In-Memory backend to be used for testing
    pub struct InMemoryBackend {
        map: RwLock<EnumMap<FileType, BTreeMap<Id, Bytes>>>,
        is_cold: bool,
        warm: RwLock<EnumMap<FileType, BTreeSet<Id>>>,
    }

    impl Clone for InMemoryBackend {
        fn clone(&self) -> Self {
            let inner_map = self.map.read().unwrap();
            let inner_warm = self.warm.read().unwrap();
            Self {
                map: RwLock::new(EnumMap::from_fn(|tpe| inner_map[tpe].clone())),
                is_cold: self.is_cold,
                warm: RwLock::new(EnumMap::from_fn(|tpe| inner_warm[tpe].clone())),
            }
        }
    }

    impl InMemoryBackend {
        /// Create a new (empty) `InMemoryBackend`
        #[must_use]
        pub fn new() -> Self {
            Self {
                map: RwLock::new(EnumMap::from_fn(|_| BTreeMap::new())),
                is_cold: false,
                warm: RwLock::new(EnumMap::from_fn(|_| BTreeSet::new())),
            }
        }

        /// Create a new (empty) cold `InMemoryBackend`
        #[must_use]
        pub fn new_cold() -> Self {
            Self {
                map: RwLock::new(EnumMap::from_fn(|_| BTreeMap::new())),
                is_cold: true,
                warm: RwLock::new(EnumMap::from_fn(|_| BTreeSet::new())),
            }
        }
    }

    impl Default for InMemoryBackend {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ReadBackend for InMemoryBackend {
        fn location(&self) -> String {
            "test".to_string()
        }

        fn list_with_size(&self, tpe: FileType) -> RusticResult<Vec<(Id, u32)>> {
            Ok(self.map.read().unwrap()[tpe]
                .iter()
                .map(|(id, byte)| {
                    (
                        *id,
                        u32::try_from(byte.len()).expect("byte length is too large"),
                    )
                })
                .collect())
        }

        fn read_full(&self, tpe: FileType, id: &Id) -> RusticResult<Bytes> {
            if self.is_cold && !self.warm.read().unwrap()[tpe].contains(id) {
                return Err(RusticError::new(
                    ErrorKind::Backend,
                    "tpe {tpe} id `{id}` is not warmed-up",
                )
                .attach_context("tpe", tpe.to_string())
                .attach_context("id", id.to_string()));
            }
            Ok(self.map.read().unwrap()[tpe]
                .get(id)
                .ok_or_else(|| {
                    RusticError::new(
                        ErrorKind::Backend,
                        "Element tpe: {tpe}, id: {id} does not exist in backend",
                    )
                    .attach_context("tpe", tpe.to_string())
                    .attach_context("id", id.to_string())
                })?
                .clone())
        }

        fn read_partial(
            &self,
            tpe: FileType,
            id: &Id,
            _cacheable: bool,
            offset: u32,
            length: u32,
        ) -> RusticResult<Bytes> {
            if self.is_cold && !self.warm.read().unwrap()[tpe].contains(id) {
                return Err(RusticError::new(
                    ErrorKind::Backend,
                    "tpe {tpe} id `{id}` is not warmed-up",
                )
                .attach_context("tpe", tpe.to_string())
                .attach_context("id", id.to_string()));
            }
            Ok(
                self.map.read().unwrap()[tpe][id]
                    .slice(offset as usize..(offset + length) as usize),
            )
        }

        fn warmup_path(&self, tpe: FileType, id: &Id) -> String {
            // For in-memory backend, return a simple identifier
            // Since this is a testing backend, we can return a formatted path
            let hex_id = id.to_hex();
            match tpe {
                FileType::Config => "config".to_string(),
                FileType::Pack => format!("data/{}/{}", &hex_id[0..2], hex_id.as_str()),
                _ => format!("{}/{}", tpe.dirname(), hex_id.as_str()),
            }
        }

        fn needs_warm_up(&self) -> bool {
            self.is_cold
        }

        fn warm_up(&self, tpe: FileType, id: &Id) -> RusticResult<()> {
            if self.is_cold {
                _ = self.warm.write().unwrap()[tpe].insert(*id);
            }
            Ok(())
        }
    }

    impl WriteBackend for InMemoryBackend {
        fn create(&self) -> RusticResult<()> {
            Ok(())
        }

        fn write_bytes(
            &self,
            tpe: FileType,
            id: &Id,
            _cacheable: bool,
            content: BytesList,
        ) -> RusticResult<()> {
            let mut bytes = BytesMut::new();
            for input in content.slice() {
                bytes.extend_from_slice(input);
            }
            let bytes = bytes.freeze();
            if self.map.write().unwrap()[tpe].insert(*id, bytes).is_some() {
                return Err(
                    RusticError::new(ErrorKind::Backend, "ID `{id}` already exists.")
                        .attach_context("id", id.to_string()),
                );
            }

            Ok(())
        }

        fn remove(&self, tpe: FileType, id: &Id, _cacheable: bool) -> RusticResult<()> {
            if self.map.write().unwrap()[tpe].remove(id).is_none() {
                return Err(
                    RusticError::new(ErrorKind::Backend, "ID `{id}` does not exist.")
                        .attach_context("id", id.to_string()),
                );
            }
            Ok(())
        }
    }
}

/// Backend that injects faults into the calls of another backend, for tests
pub mod fault_injection_backend {
    use std::sync::{Arc, PoisonError, RwLock};

    use bytes::Bytes;

    use rustic_core::{
        BytesList, ErrorKind, FileType, Id, ReadBackend, RusticError, RusticResult, WriteBackend,
    };

    /// The start of the message of each error that [`FaultInjectionBackend`] injects.
    pub const INJECTED_FAULT: &str = "Injected fault";

    /// An operation of a backend
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum BackendOp {
        /// Lists the files of a type.
        List,
        /// Reads a full file.
        ReadFull,
        /// Reads a part of a file.
        ReadPartial,
        /// Writes a file.
        Write,
        /// Removes a file.
        Remove,
    }

    /// A call to a backend
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BackendCall {
        /// The operation of the call.
        pub op: BackendOp,
        /// The type of the file.
        pub tpe: FileType,
        /// The ID of the file. A listing has no ID.
        pub id: Option<Id>,
    }

    /// A fault that [`FaultInjectionBackend`] injects into a call
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Fault {
        /// The call returns an error. The inner backend does not get the call.
        Error,
        /// The backend inverts the last byte of the data that a read returns.
        ///
        /// A read that gets no data returns an error.
        /// A listing, a write and a removal return an error, as with [`Fault::Error`].
        Corrupt,
    }

    /// The rule that decides the fault for each call.
    type Rule = Box<dyn Fn(&BackendCall) -> Option<Fault> + Send + Sync>;

    /// Backend that injects faults into the calls of an inner backend
    ///
    /// A rule decides the fault for each call.
    /// A call without a fault goes to the inner backend without change.
    pub struct FaultInjectionBackend {
        /// The backend that gets the calls without a fault.
        inner: Arc<dyn WriteBackend>,
        /// The rule that decides the fault for each call.
        rule: RwLock<Option<Rule>>,
    }

    impl std::fmt::Debug for FaultInjectionBackend {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FaultInjectionBackend")
                .field("inner", &self.inner)
                .finish_non_exhaustive()
        }
    }

    impl FaultInjectionBackend {
        /// Create a new `FaultInjectionBackend` that sends each call to `inner`, without faults
        ///
        /// # Arguments
        ///
        /// * `inner` - The backend that gets the calls without a fault
        #[must_use]
        pub fn new(inner: Arc<dyn WriteBackend>) -> Self {
            Self {
                inner,
                rule: RwLock::new(None),
            }
        }

        /// Sets the rule that decides the fault for each call after this call.
        ///
        /// The rule replaces the previous rule.
        /// For a call without a fault, the rule returns `None`.
        /// The rule must not call [`Self::inject`] or [`Self::clear`].
        ///
        /// # Arguments
        ///
        /// * `rule` - The rule that decides the fault for each call
        pub fn inject(&self, rule: impl Fn(&BackendCall) -> Option<Fault> + Send + Sync + 'static) {
            *self.rule.write().unwrap_or_else(PoisonError::into_inner) = Some(Box::new(rule));
        }

        /// Removes the rule. The calls after this call go to the inner backend without change.
        pub fn clear(&self) {
            *self.rule.write().unwrap_or_else(PoisonError::into_inner) = None;
        }

        /// Gives the fault that the rule decides for `call`.
        fn fault(&self, call: &BackendCall) -> Option<Fault> {
            self.rule
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
                .and_then(|rule| rule(call))
        }

        /// Returns an error if the rule decides a fault for `call`.
        ///
        /// # Errors
        ///
        /// * If the rule decides a fault for `call`.
        fn check(&self, call: &BackendCall) -> RusticResult<()> {
            self.fault(call)
                .map_or(Ok(()), |_| Err(injected_error(call)))
        }

        /// Reads with `read`, and injects the fault that the rule decides for `call`.
        ///
        /// # Errors
        ///
        /// * If the rule decides [`Fault::Error`] for `call`.
        /// * If the rule decides [`Fault::Corrupt`] for `call`, and the read gets no data.
        /// * If `read` fails.
        fn read(
            &self,
            call: &BackendCall,
            read: impl FnOnce() -> RusticResult<Bytes>,
        ) -> RusticResult<Bytes> {
            match self.fault(call) {
                None => read(),
                Some(Fault::Error) => Err(injected_error(call)),
                Some(Fault::Corrupt) => corrupt(read()?).ok_or_else(|| injected_error(call)),
            }
        }
    }

    /// Creates the error for a call with a fault.
    fn injected_error(call: &BackendCall) -> Box<RusticError> {
        RusticError::new(
            ErrorKind::Backend,
            "Injected fault in the call `{op}` for the file type `{tpe}` and the ID `{id}`.",
        )
        .attach_context("op", format!("{:?}", call.op))
        .attach_context("tpe", call.tpe.to_string())
        .attach_context(
            "id",
            call.id
                .map_or_else(|| "none".to_string(), |id| id.to_string()),
        )
    }

    /// Inverts the last byte of `data`.
    ///
    /// # Returns
    ///
    /// The changed data, or `None` if `data` is empty.
    fn corrupt(data: Bytes) -> Option<Bytes> {
        let mut data = Vec::from(data);
        let last = data.last_mut()?;
        *last = !*last;
        Some(data.into())
    }

    impl ReadBackend for FaultInjectionBackend {
        fn location(&self) -> String {
            self.inner.location()
        }

        fn list_with_size(&self, tpe: FileType) -> RusticResult<Vec<(Id, u32)>> {
            self.check(&BackendCall {
                op: BackendOp::List,
                tpe,
                id: None,
            })?;
            self.inner.list_with_size(tpe)
        }

        fn list(&self, tpe: FileType) -> RusticResult<Vec<Id>> {
            self.check(&BackendCall {
                op: BackendOp::List,
                tpe,
                id: None,
            })?;
            self.inner.list(tpe)
        }

        fn read_full(&self, tpe: FileType, id: &Id) -> RusticResult<Bytes> {
            let call = BackendCall {
                op: BackendOp::ReadFull,
                tpe,
                id: Some(*id),
            };
            self.read(&call, || self.inner.read_full(tpe, id))
        }

        fn read_partial(
            &self,
            tpe: FileType,
            id: &Id,
            cacheable: bool,
            offset: u32,
            length: u32,
        ) -> RusticResult<Bytes> {
            let call = BackendCall {
                op: BackendOp::ReadPartial,
                tpe,
                id: Some(*id),
            };
            self.read(&call, || {
                self.inner.read_partial(tpe, id, cacheable, offset, length)
            })
        }

        fn warmup_path(&self, tpe: FileType, id: &Id) -> String {
            self.inner.warmup_path(tpe, id)
        }

        fn needs_warm_up(&self) -> bool {
            self.inner.needs_warm_up()
        }

        fn warm_up(&self, tpe: FileType, id: &Id) -> RusticResult<()> {
            self.inner.warm_up(tpe, id)
        }
    }

    impl WriteBackend for FaultInjectionBackend {
        fn create(&self) -> RusticResult<()> {
            self.inner.create()
        }

        fn write_bytes(
            &self,
            tpe: FileType,
            id: &Id,
            cacheable: bool,
            content: BytesList,
        ) -> RusticResult<()> {
            self.check(&BackendCall {
                op: BackendOp::Write,
                tpe,
                id: Some(*id),
            })?;
            self.inner.write_bytes(tpe, id, cacheable, content)
        }

        fn remove(&self, tpe: FileType, id: &Id, cacheable: bool) -> RusticResult<()> {
            self.check(&BackendCall {
                op: BackendOp::Remove,
                tpe,
                id: Some(*id),
            })?;
            self.inner.remove(tpe, id, cacheable)
        }
    }

    #[cfg(test)]
    mod tests {
        use std::sync::{Arc, Mutex};

        use bytes::Bytes;
        use rustic_core::{FileType, Id, ReadBackend, RusticResult, WriteBackend};

        use super::{BackendCall, BackendOp, Fault, FaultInjectionBackend, INJECTED_FAULT};
        use crate::backend::in_memory_backend::InMemoryBackend;

        /// Creates a backend with one pack file that holds `data`.
        fn backend_with_pack(data: &'static [u8]) -> (FaultInjectionBackend, Id) {
            let backend = FaultInjectionBackend::new(Arc::new(InMemoryBackend::new()));
            let id = Id::random();
            backend
                .write_bytes(FileType::Pack, &id, false, Bytes::from_static(data).into())
                .unwrap();
            (backend, id)
        }

        /// Tells if `result` is an error that the backend injected.
        fn is_injected<T>(result: RusticResult<T>) -> bool {
            result.is_err_and(|err| err.to_string().contains(INJECTED_FAULT))
        }

        #[test]
        fn calls_without_a_rule_reach_the_inner_backend() {
            let (backend, id) = backend_with_pack(b"pack data");
            assert_eq!(backend.list(FileType::Pack).unwrap(), vec![id]);
            assert_eq!(
                backend.list_with_size(FileType::Pack).unwrap(),
                vec![(id, 9)]
            );
            assert_eq!(
                backend.read_full(FileType::Pack, &id).unwrap(),
                Bytes::from_static(b"pack data")
            );
            assert_eq!(
                backend
                    .read_partial(FileType::Pack, &id, false, 5, 4)
                    .unwrap(),
                Bytes::from_static(b"data")
            );
            backend.remove(FileType::Pack, &id, false).unwrap();
            assert!(backend.list(FileType::Pack).unwrap().is_empty());
        }

        #[test]
        fn error_fails_each_operation_and_changes_nothing() {
            let (backend, id) = backend_with_pack(b"pack data");
            backend.inject(|_| Some(Fault::Error));
            assert!(is_injected(backend.list(FileType::Pack)));
            assert!(is_injected(backend.list_with_size(FileType::Pack)));
            assert!(is_injected(backend.read_full(FileType::Pack, &id)));
            assert!(is_injected(backend.read_partial(
                FileType::Pack,
                &id,
                false,
                0,
                4
            )));
            assert!(is_injected(backend.write_bytes(
                FileType::Pack,
                &Id::random(),
                false,
                Bytes::from_static(b"other").into()
            )));
            assert!(is_injected(backend.remove(FileType::Pack, &id, false)));

            backend.clear();
            assert_eq!(backend.list(FileType::Pack).unwrap(), vec![id]);
        }

        #[test]
        fn corrupt_inverts_the_last_byte_of_a_read() {
            let (backend, id) = backend_with_pack(b"pack data");
            backend.inject(|_| Some(Fault::Corrupt));
            assert_eq!(
                backend.read_full(FileType::Pack, &id).unwrap(),
                Bytes::from_static(b"pack dat\x9e")
            );
            assert_eq!(
                backend
                    .read_partial(FileType::Pack, &id, false, 0, 4)
                    .unwrap(),
                Bytes::from_static(b"pac\x94")
            );

            backend.clear();
            assert_eq!(
                backend.read_full(FileType::Pack, &id).unwrap(),
                Bytes::from_static(b"pack data")
            );
        }

        #[test]
        fn corrupt_fails_a_read_of_no_data() {
            let (backend, id) = backend_with_pack(b"");
            backend.inject(|_| Some(Fault::Corrupt));
            assert!(is_injected(backend.read_full(FileType::Pack, &id)));
            assert!(is_injected(backend.read_partial(
                FileType::Pack,
                &id,
                false,
                0,
                0
            )));
        }

        #[test]
        fn corrupt_fails_listings_writes_and_removals() {
            let (backend, id) = backend_with_pack(b"pack data");
            backend.inject(|_| Some(Fault::Corrupt));
            assert!(is_injected(backend.list(FileType::Pack)));
            assert!(is_injected(backend.list_with_size(FileType::Pack)));
            assert!(is_injected(backend.write_bytes(
                FileType::Pack,
                &Id::random(),
                false,
                Bytes::from_static(b"other").into()
            )));
            assert!(is_injected(backend.remove(FileType::Pack, &id, false)));

            backend.clear();
            assert_eq!(backend.list(FileType::Pack).unwrap(), vec![id]);
        }

        #[test]
        fn rule_gets_each_call() {
            let (backend, id) = backend_with_pack(b"pack data");
            let calls = Arc::new(Mutex::new(Vec::new()));
            let seen = Arc::clone(&calls);
            backend.inject(move |call| {
                seen.lock().unwrap().push(*call);
                None
            });

            _ = backend.list(FileType::Index).unwrap();
            _ = backend.read_full(FileType::Pack, &id).unwrap();
            _ = backend
                .read_partial(FileType::Pack, &id, false, 0, 4)
                .unwrap();
            backend.remove(FileType::Pack, &id, false).unwrap();

            let call = |op, tpe, id| BackendCall { op, tpe, id };
            assert_eq!(
                *calls.lock().unwrap(),
                vec![
                    call(BackendOp::List, FileType::Index, None),
                    call(BackendOp::ReadFull, FileType::Pack, Some(id)),
                    call(BackendOp::ReadPartial, FileType::Pack, Some(id)),
                    call(BackendOp::Remove, FileType::Pack, Some(id)),
                ]
            );
        }
    }
}
