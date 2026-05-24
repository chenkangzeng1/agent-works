use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct McpToolInfo {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

#[derive(Clone, Debug)]
pub enum McpTransport {
    Http { url: String },
    Stdio { command: String, args: Vec<String> },
}

#[derive(Clone, Debug)]
pub struct McpServerConfig {
    pub name: String,
    pub transport: McpTransport,
    pub auto_reconnect: bool,
}
