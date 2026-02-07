//! Stub implementation of asupersync::cx::Cx.

use std::marker::PhantomData;

#[derive(Debug, Clone, Copy)]
pub struct Cx<Caps = ()> {
    _marker: PhantomData<Caps>,
}

impl<Caps> Cx<Caps> {
    pub fn new() -> Self {
        Self { _marker: PhantomData }
    }

    pub fn checkpoint(&self) -> std::result::Result<(), i32> {
        Ok(())
    }

    pub fn checkpoint_with(&self, _msg: &str) -> std::result::Result<(), i32> {
        Ok(())
    }

    pub fn restrict<NewCaps>(&self) -> Cx<NewCaps> {
        Cx { _marker: PhantomData }
    }
}