#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Capacity(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Reached(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum CapacityRefusal {
    Zero,
    Negative,
}
#[rustfmt::skip]
pub trait Subscribable {
    type State;
    type Change;
    fn state(&self) -> Self::State;
}
#[rustfmt::skip]
pub trait Publishing {
    type State;
    type Change;
    fn create(
        input_0: Self::State,
        input_1: Capacity,
    ) -> std::result::Result<Self, CapacityRefusal>
    where
        Self: Sized;
    fn publish(&self, input: Self::Change) -> Reached;
    fn open(&self) -> signal::Stream<Self::State, Self::Change>;
}
