

pub trait MakesReporter: Clone + Send + Sync + 'static {
    type Instrumentation: Reporter;

    fn make(&self) -> Self::Instrumentation;
}

impl MakesReporter for () {
    type Instrumentation = ();

    fn make(&self) {

    }
}

pub trait Reporter: Clone + Send + Sync + 'static {}

impl Reporter for () {}
