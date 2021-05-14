#[doc(hidden)]
#[allow(unused_macros)]
macro_rules! set {
    () => (HashSet::new());
    ( $( $x:expr ),+ $(,)?) => {  // Match one or more comma delimited items
        {
            let mut temp_set = HashSet::new();  // Create a mutable HashSet
            $(
                temp_set.insert($x); // Insert each item matched into the HashSet
            )*
            temp_set // Return the populated HashSet
        }
    };
}

#[doc(hidden)]
#[allow(unused_macros)]
macro_rules! result_to_error {
    ($value:expr) => {
        match $value {
            Ok(v) => v,
            Err(e) => return e,
        }
    };
    ($value:expr, $error:expr) => {
        match $value {
            Ok(v) => v,
            Err(_) => return $error,
        }
    };
}

#[doc(hidden)]
#[allow(unused_macros)]
macro_rules! option_to_error {
    ($value:expr) => {
        match $value {
            Some(v) => v,
            None => return,
        }
    };
    ($value:expr, $error:expr) => {
        match $value {
            Some(v) => v,
            None => return $error,
        }
    };
}
