#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
pub type Pid = i64;
#[rustfmt::skip]
pub type Started = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Process {
    pub pid: Pid,
    pub started: Started,
}
#[rustfmt::skip]
pub type Ancestry = std::vec::Vec<Process>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Unidentified {
    PeerMismatch,
    Vanished,
    Reused,
    Malformed,
}
#[rustfmt::skip]
pub trait Identifiable {
    fn identify(&self) -> std::result::Result<Ancestry, Unidentified>;
}
#[rustfmt::skip]
const _: () = {
    fn assert_process_identifiable<T: Identifiable>() {}
    let _ = assert_process_identifiable::<Process>;
};
