use std::error::Error;
use std::fmt::{self, Display};

pub(crate) struct ErrorReport<'a>(&'a (dyn Error + 'static));

impl<'a> ErrorReport<'a> {
    pub(crate) fn new(error: &'a (dyn Error + 'static)) -> Self {
        Self(error)
    }
}

impl Display for ErrorReport<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)?;
        let mut source = self.0.source();
        while let Some(error) = source {
            write!(formatter, ": {error}")?;
            source = error.source();
        }
        Ok(())
    }
}
