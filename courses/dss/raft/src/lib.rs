#[allow(unused_imports)]
#[macro_use]
extern crate log;
#[allow(unused_imports)]
#[macro_use]
extern crate prost_derive;

#[allow(unused_imports)]
pub mod kvraft;
#[allow(unused_imports)]
mod proto;
#[allow(unused_imports)]
pub mod raft;

/// A place holder for suppressing unused_variables warning.
fn your_code_here<T>(_: T) -> ! {
    unimplemented!()
}
