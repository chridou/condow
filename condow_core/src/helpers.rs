pub(crate) const CONDOW_PREFIX: &str = "CONDOW";

macro_rules! env_funs {
    ($var:expr) => {
        #[doc="The default name of the environment variable for this type.\n\n"]
        #[doc="The name of the environment variable is \""]
        #[doc=$var]
        #[doc="\""]
        pub const ENV_TYPE_NAME: &'static str = &$var;

        #[doc="Initialize from the environment.\n"]
        #[doc="Returns `None` if the value was not found and fails if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is \"CONDOW_"]
        #[doc=$var]
        #[doc="\""]
        pub fn try_from_env() -> Result<Option<Self>, anyhow::Error> {
            Self::try_from_env_prefixed($crate::helpers::CONDOW_PREFIX)
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Returns `None` if the value was not found and fails if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is \"`prefix`_"]
        #[doc=$var]
        #[doc="\"\n\n"]
        #[doc="The underscore and prefix will be omitted if prefix is empty."]
        pub fn try_from_env_prefixed<T: Into<String>>(
            prefix: T,
        ) -> Result<Option<Self>, anyhow::Error> {
            let mut var_name: String = prefix.into();
            if !var_name.is_empty() {
                var_name.push('_');
            }
            var_name.push_str(&$var);
            Self::try_from_env_named(var_name)
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Returns `None` if the value was not found and fails if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is `var_name`."]
         pub fn try_from_env_named<T: AsRef<str>>(
            var_name: T,
        ) -> Result<Option<Self>, anyhow::Error> {
            match std::env::var(var_name.as_ref()) {
                Ok(value) => value.parse().map(Some).map_err(|err| {
                    anyhow::Error::msg(format!(
                        "could not parse env var '{}': {}",
                        var_name.as_ref(),
                        err
                    ))
                }),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(std::env::VarError::NotUnicode(_)) => Err(anyhow::Error::msg(format!(
                    "env var '{}' is not unicode",
                    var_name.as_ref()
                ))),
            }
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Returns `None` if the value was not found and fails if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is \""]
        #[doc=$var]
        #[doc="\""]
        pub fn try_from_env_type_name() -> Result<Option<Self>, anyhow::Error> {
            Self::try_from_env_named(Self::ENV_TYPE_NAME)
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Fails if the value was not found or if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is \"CONDOW_"]
        #[doc=$var]
        #[doc="\""]
         pub fn from_env() -> Result<Self, anyhow::Error> {
            Self::from_env_prefixed($crate::helpers::CONDOW_PREFIX)
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Fails if the value was not found or if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is \"`prefix`_"]
        #[doc=$var]
        #[doc="\"\n\n"]
        #[doc="The underscore and prefix will be omitted if prefix is empty."]
        pub fn from_env_prefixed<T: Into<String>>(prefix: T) -> Result<Self, anyhow::Error> {
            let mut var_name: String = prefix.into();
            if !var_name.is_empty() {
                var_name.push('_');
            }
            var_name.push_str(&$var);
            Self::from_env_named(var_name)
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Fails if the value was not found or if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is `var_name`."]
        pub fn from_env_named<T: AsRef<str>>(var_name: T) -> Result<Self, anyhow::Error> {
            Self::try_from_env_named(var_name.as_ref()).and_then(|v| {
                v.map(Ok).unwrap_or_else(|| {
                    Err(anyhow::Error::msg(format!(
                        "env var '{}' not found",
                        var_name.as_ref()
                    )))
                })
            })
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Fails if the value was not found or if the value could not be parsed.\n"]
        #[doc="The name of the environment variable is \""]
        #[doc=$var]
        #[doc="\""]
        pub fn from_env_type_name() -> Result<Self, anyhow::Error> {
            Self::from_env_named(Self::ENV_TYPE_NAME)
        }


        #[doc="Initialize from the environment.\n"]
        #[doc="Returns `None` if the value could not be read for any reason.\n"]
        #[doc="The name of the environment variable is \"CONDOW_"]
        #[doc=$var]
        #[doc="\""]
         pub fn from_env_opt() -> Option<Self> {
            Self::from_env_prefixed($crate::helpers::CONDOW_PREFIX).ok()
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Returns `None` if the value could not be read for any reason.\n"]
        #[doc="The name of the environment variable is \"`prefix`_"]
        #[doc=$var]
        #[doc="\"\n\n"]
        #[doc="The underscore and prefix will be omitted if prefix is empty."]
         pub fn from_env_opt_prefixed<T: Into<String>>(prefix: T) -> Option<Self> {
            let mut var_name: String = prefix.into();
            if !var_name.is_empty() {
                var_name.push('_');
            }
            var_name.push_str(&$var);
            Self::from_env_named(var_name).ok()
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Returns `None` if the value could not be read for any reason.\n"]
        #[doc="The name of the environment variable is `var_name`."]
        pub fn from_env_opt_named<T: AsRef<str>>(var_name: T) -> Option<Self> {
            Self::from_env_named(var_name.as_ref()).ok()
        }

        #[doc="Initialize from the environment.\n"]
        #[doc="Returns `None` if the value could not be read for any reason.\n"]
        #[doc="The name of the environment variable is \""]
        #[doc=$var]
        #[doc="\""]
        pub fn from_env_opt_type_name() -> Option<Self> {
            Self::from_env_opt_named(Self::ENV_TYPE_NAME)
        }
    };
}

macro_rules! __new_type_base {
    ($(#[$outer:meta])*; $Name:ident; $T:ty) => {
        $(#[$outer])*
        pub struct $Name($T);

        impl $Name {
            pub fn new<T: Into<$T>>(v: T) -> Self {
                Self(v.into())
            }
        }

        impl std::fmt::Display for $Name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::write!(f, "{}", self.0)
            }
        }

        impl From<$T> for $Name {
            fn from(v: $T) -> $Name {
                $Name(v)
            }
        }

        impl From<$Name> for $T {
            fn from(v: $Name) -> $T {
                v.0
            }

        }

        impl std::str::FromStr for $Name {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok($Name(s.parse().map_err(|err| {
                    anyhow::Error::msg(std::format!("could not parse {}: {}", s, err))
                })?))
            }
        }
    }
}

macro_rules! __new_type_base_copy_ext {
    ($Name:ident; $T:ty) => {
        impl $Name {
            pub fn into_inner(self) -> $T {
                self.0
            }
        }
    };
}

macro_rules! __new_type_base_clone_ext {
    ($Name:ident; $T:ty) => {
        impl $Name {
            pub fn into_inner(self) -> $T {
                self.0
            }

            pub fn as_ref(self) -> &$T {
                &self.0
            }
        }
    };
}

macro_rules! __new_type_base_string_ext {
    ($Name:ident) => {
        impl $Name {
            pub fn into_inner(self) -> String {
                self.0
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn as_bytes(&self) -> &[u8] {
                self.0.as_bytes()
            }
        }

        impl From<&str> for $Name {
            fn from(v: &str) -> $Name {
                $Name::new(v)
            }
        }

        impl AsRef<str> for $Name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl AsRef<[u8]> for $Name {
            fn as_ref(&self) -> &[u8] {
                self.as_bytes()
            }
        }
    };
}

macro_rules! __new_type_base_uuid_ext {
    ($Name:ident) => {
        impl $Name {
            pub fn to_inner(self) -> uuid::Uuid {
                self.0
            }

            pub fn as_bytes(self) -> &[u8] {
                self.0.as_bytes()
            }
        }

        impl AsRef<[u8]> for $Name {
            fn as_ref(&self) -> &[u8] {
                self.as_bytes()
            }
        }
    };
}

macro_rules! new_type {
    ($(#[$outer:meta])* pub struct $Name:ident(String);) => {
        __new_type_base!($(#[$outer])*;$Name;String);
        __new_type_base_string_ext!($Name);
    };
    ($(#[$outer:meta])* pub struct $Name:ident(Uuid);) => {
        __new_type_base!($(#[$outer])*;$Name;Uuid);
        __new_type_base_uuid_ext!($Name);
    };
    ($(#[$outer:meta])* pub struct $Name:ident($T:ty);) => {
        __new_type_base!($(#[$outer])*;$Name;$T);
        __new_type_base_clone_ext!($Name;$T);
    };
    ($(#[$outer:meta])* pub copy struct $Name:ident($T:ty);) => {
        __new_type_base!($(#[$outer])*;$Name;$T);
        __new_type_base_copy_ext!($Name;$T);
    };
    ($(#[$outer:meta])* pub struct $Name:ident(String, env=$env:expr);) => {
        __new_type_base!($(#[$outer])*;$Name;String);
        __new_type_base_string_ext!($Name);
        impl $Name {
            env_funs!($env);
        }
    };
    ($(#[$outer:meta])* pub struct $Name:ident(Uuid, env=$env:expr);) => {
        __new_type_base!($(#[$outer])*;$Name;Uuid);
        __new_type_base_uuid_ext!($Name);
        impl $Name {
            env_funs!($env);
        }
    };
    ($(#[$outer:meta])* pub struct $Name:ident($T:ty, env=$env:expr);) => {
        __new_type_base!($(#[$outer])*;$Name;$T);
        __new_type_base_clone_ext!($Name;$T);
        impl $Name {
            env_funs!($env);
        }
    };
    ($(#[$outer:meta])* pub copy struct $Name:ident($T:ty, env=$env:expr);) => {
        __new_type_base!($(#[$outer])*;$Name;$T);
        __new_type_base_copy_ext!($Name;$T);
        impl $Name {
            env_funs!($env);
        }
    };
    ($(#[$outer:meta])* pub secs struct $Name:ident($T:ty, env=$env:expr);) => {
        __new_type_base!($(#[$outer])*;$Name;$T);
        __new_type_base_copy_ext!($Name;$T);
        impl $Name {
            env_funs!($env);

            pub fn into_duration(self) -> Duration {
                Duration::from_secs(u64::from(self.0))
            }
        }

        impl From<$Name> for Duration {
            fn from(v: $Name) -> Duration {
                v.into_duration()
            }
        }
    };
    ($(#[$outer:meta])* pub millis struct $Name:ident($T:ty, env=$env:expr);) => {
        __new_type_base!($(#[$outer])*;$Name;$T);
        __new_type_base_copy_ext!($Name;$T);
        impl $Name {
            env_funs!($env);

            pub fn into_duration(self) -> Duration {
                Duration::from_millis(u64::from(self.0))
            }
        }

        impl From<$Name> for Duration {
            fn from(v: $Name) -> Duration {
                v.into_duration()
            }
        }
    };
}

// pub fn mandatory<T>(v: Option<T>, field_name: &'static str) -> Result<T, anyhow::Error> {
//     if let Some(v) = v {
//         Ok(v)
//     } else {
//         Err(anyhow::Error::msg(format!(
//             "field '{}' is mandatory",
//             field_name
//         )))
//     }
// }

macro_rules! env_ctors {
    (no_fill) => {
        #[doc="Initializes all fields from environment variables prefixed with \"CONDOW_\""]
        pub fn from_env() -> Result<Self, anyhow::Error> {
            let mut me = Self::default();
            me.fill_from_env_prefixed_internal($crate::helpers::CONDOW_PREFIX)?;
            Ok(me)
        }

        #[doc="Initializes all fields from environment variables prefixed with \"[prefix]_\"\n\n"]
        #[doc="The underscore is omitted if `prefix` is empty"]
        pub fn from_env_prefixed<T: AsRef<str>>(prefix: T) -> Result<Self, anyhow::Error> {
            let mut me = Self::default();
            me.fill_from_env_prefixed_internal(prefix)?;
            Ok(me)
        }

        #[doc="Initializes all fields from environment variables without any prefix"]
        pub fn from_env_type_names() -> Result<Self, anyhow::Error> {
            let mut me = Self::default();
            me.fill_from_env_prefixed_internal("")?;
            Ok(me)
        }
    };

    () => {
        env_ctors!(no_fill);
        #[doc="Updates all not yet set fields from environment variables prefixed with \"CONDOW_\""]
        pub fn fill_from_env(&mut self) -> Result<(), anyhow::Error> {
            self.fill_from_env_prefixed_internal($crate::helpers::CONDOW_PREFIX)
        }

        #[doc="Updates all not yet set fields from environment variables prefixed with \"[prefix]_\"\n\n"]
        #[doc="The underscore is omitted if `prefix` is empty"]
        pub fn fill_from_env_prefixed<T: AsRef<str>>(&mut self, prefix: T) -> Result<(), anyhow::Error> {
            self.fill_from_env_prefixed_internal(prefix)
        }

        #[doc="Updates all not yet set fields from environment variables without any prefix"]
        pub fn fill_from_env_type_names(&mut self) -> Result<(), anyhow::Error> {
            self.fill_from_env_prefixed_internal("")
        }
    };
}
