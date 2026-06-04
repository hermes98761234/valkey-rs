use bytes::Bytes;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    NilBulk,
    Nil,
    Int(i64),
    Bulk(Bytes),
    SimpleString(String),
    Error(String),
    Array(Vec<Value>),
}

impl Value {
    pub fn bulk(s: impl Into<Bytes>) -> Self {
        Value::Bulk(s.into())
    }

    pub fn int(i: i64) -> Self {
        Value::Int(i)
    }

    pub fn error(s: impl Into<String>) -> Self {
        Value::Error(s.into())
    }

    pub fn ok() -> Self {
        Value::SimpleString("OK".to_string())
    }

    pub fn array(v: Vec<Value>) -> Self {
        Value::Array(v)
    }

    pub fn empty_array() -> Self {
        Value::Array(Vec::new())
    }

    pub fn is_nil(&self) -> bool {
        matches!(self, Value::NilBulk | Value::Nil)
    }
}
