use std::{
    fmt,
    ops::{Deref, DerefMut},
};

/// S3 bucket name
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bucket(String);

impl Bucket {
    pub fn new<T: Into<String>>(bucket: T) -> Self {
        Self(bucket.into())
    }

    pub fn object<O: Into<ObjectKey>>(self, key: O) -> S3Location {
        S3Location(self, key.into())
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for Bucket {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl Deref for Bucket {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Bucket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// S3 object key
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn new<T: Into<String>>(key: T) -> Self {
        Self(key.into())
    }

    pub fn in_bucket<B: Into<Bucket>>(self, bucket: B) -> S3Location {
        S3Location(bucket.into(), self)
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for ObjectKey {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ObjectKey {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<&str> for ObjectKey {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

/// Full "path" to an S3 object
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct S3Location(Bucket, ObjectKey);

impl S3Location {
    pub fn new<B: Into<Bucket>, O: Into<ObjectKey>>(bucket: B, key: O) -> Self {
        Self(bucket.into(), key.into())
    }

    pub fn bucket(&self) -> &Bucket {
        &self.0
    }

    pub fn key(&self) -> &ObjectKey {
        &self.1
    }

    /// Turn this into its two components
    pub fn into_inner(self) -> (Bucket, ObjectKey) {
        (self.0, self.1)
    }
}

impl fmt::Display for S3Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s3://{}/{}", self.0, self.1)
    }
}
