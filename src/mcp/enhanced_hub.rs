use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use agent_base::{AgentResult, ToolRegistry, AgentError};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{info, warn, error};

use super::client::{McpClient, McpToolAdapter};
use super::types::{McpServerConfig, McpToolInfo, McpTransport};

#[derive(Clone, Debug)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Failed(String),
    Unhealthy(String),
}

struct ServerEntry {
    config: McpServerConfig,
    clients: RwLock<Vec<Arc<McpClient>>>,
    max_connections: usize,
    tools: RwLock<Vec<McpToolInfo>>,
    state: RwLock<ConnectionState>,
    last_health_check: RwLock<Option<Instant>>,
    reconnect_attempts: RwLock<u32>,
}

impl ServerEntry {
    fn new(config: McpServerConfig) -> Self {
        Self {
            config,
            clients: RwLock::new(Vec::new()),
            max_connections: 5, // 默认最大连接数
            tools: RwLock::new(Vec::new()),
            state: RwLock::new(ConnectionState::Disconnected),
            last_health_check: RwLock::new(None),
            reconnect_attempts: RwLock::new(0),
        }
    }

    async fn create_client(&self) -> AgentResult<Arc<McpClient>> {
        let client = Arc::new(McpClient::new(self.config.transport.clone()).await?);
        Ok(client)
    }

    async fn get_available_client(&self) -> Option<Arc<McpClient>> {
        let clients = self.clients.read().await;
        if clients.is_empty() {
            return None;
        }

        // 返回第一个可用客户端，实际实现中可以使用更复杂的负载均衡策略
        clients.first().cloned()
    }

    async fn add_client(&self) -> AgentResult<()> {
        if self.clients.read().await.len() >= self.max_connections {
            return Err(AgentError::resource_unavailable(
                "Maximum connections reached".to_string(),
            ));
        }

        let client = self.create_client().await?;
        self.clients.write().await.push(client);

        let mut state = self.state.write().await;
        *state = ConnectionState::Connected;

        Ok(())
    }

    async fn remove_client(&self, index: usize) {
        let mut clients = self.clients.write().await;
        if index < clients.len() {
            clients.remove(index);
        }
    }

    async fn reconnect(&self) -> AgentResult<()> {
        let mut attempts = self.reconnect_attempts.write().await;
        *attempts += 1;
        let attempt_count = *attempts;
        drop(attempts);

        // 指数退避
        let delay = Duration::from_millis(std::cmp::min(1000 * (2_u64.pow(attempt_count.min(5))), 30000));
        sleep(delay).await;

        // 清除现有连接
        self.clients.write().await.clear();

        match self.add_client().await {
            Ok(_) => {
                let mut state = self.state.write().await;
                *state = ConnectionState::Connected;

                let mut attempts = self.reconnect_attempts.write().await;
                *attempts = 0;

                info!("Successfully reconnected to MCP server: {}", self.config.name);
                Ok(())
            }
            Err(e) => {
                let mut state = self.state.write().await;
                *state = ConnectionState::Failed(e.to_string());
                error!("Failed to reconnect to {}: {e}", self.config.name);
                Err(e)
            }
        }
    }

    async fn health_check(&self) -> bool {
        if let Some(client) = self.get_available_client().await {
            match client.list_tools().await {
                Ok(_) => {
                    let mut state = self.state.write().await;
                    *state = ConnectionState::Connected;

                    let mut last_check = self.last_health_check.write().await;
                    *last_check = Some(Instant::now());

                    true
                }
                Err(e) => {
                    let mut state = self.state.write().await;
                    *state = ConnectionState::Unhealthy(e.to_string());
                    warn!("Health check failed for {}: {e}", self.config.name);
                    false
                }
            }
        } else {
            false
        }
    }
}

pub struct EnhancedMcpHub {
    servers: HashMap<String, Arc<ServerEntry>>,
    health_check_interval: Duration,
}

impl EnhancedMcpHub {
    pub fn new() -> Self {
        Self {
            servers: HashMap::new(),
            health_check_interval: Duration::from_secs(30), // 默认30秒检查一次
        }
    }

    pub fn with_health_check_interval(mut self, interval: Duration) -> Self {
        self.health_check_interval = interval;
        self
    }

    pub fn add_server(&mut self, config: McpServerConfig) {
        let entry = Arc::new(ServerEntry::new(config));
        self.servers.insert(entry.config.name.clone(), entry);
    }

    pub async fn connect_all(&self) -> AgentResult<()> {
        for (name, entry) in &self.servers {
            if let Err(e) = entry.add_client().await {
                let mut state = entry.state.write().await;
                *state = ConnectionState::Failed(e.to_string());
                error!(server_name = %name, error = %e, "Failed to connect to MCP server");
                return Err(e);
            }

            info!(server_name = %name, "Connected to MCP server");
        }

        // 启动后台健康检查任务
        self.start_health_check_task().await;

        Ok(())
    }

    pub async fn discover_all(&self) -> AgentResult<Vec<(String, Vec<McpToolInfo>)>> {
        let mut results = Vec::new();

        for (name, entry) in &self.servers {
            let Some(client) = entry.get_available_client().await else {
                warn!("Server {} not connected", name);
                continue;
            };

            match client.list_tools().await {
                Ok(tools) => {
                    {
                        let mut entry_tools = entry.tools.write().await;
                        *entry_tools = tools.clone();
                    }

                    info!("Discovered {} tools from {}", tools.len(), name);
                    results.push((name.clone(), tools));
                }
                Err(e) => {
                    warn!("Failed to discover tools from {}: {e}", name);
                    let mut state = entry.state.write().await;
                    *state = ConnectionState::Failed(format!("{e}"));
                }
            }
        }

        Ok(results)
    }

    pub fn register_all(&self, registry: &mut ToolRegistry) {
        for (name, entry) in &self.servers {
            if let Ok(tools) = futures_core::executor::block_on(async {
                entry.tools.read().await.clone()
            }) {
                for tool_info in &tools {
                    let Some(client) = futures_core::executor::block_on(entry.get_available_client()) else {
                        continue;
                    };
                    let adapter = McpToolAdapter::new(tool_info.clone(), client);
                    registry.register(adapter);
                }
            }
            info!(server_name = %name, tool_count = entry.tools.try_read().unwrap_or(&vec![]).len(), "Registered tools from MCP server");
        }
    }

    pub async fn disconnect_all(&self) {
        for (name, entry) in &self.servers {
            entry.clients.write().await.clear();
            let mut state = entry.state.write().await;
            *state = ConnectionState::Disconnected;
            info!(server_name = %name, "Disconnected from MCP server");
        }
    }

    async fn start_health_check_task(&self) {
        let servers = self.servers.clone();
        let interval = self.health_check_interval;

        tokio::spawn(async move {
            loop {
                sleep(interval).await;

                for (name, entry) in &servers {
                    let is_healthy = entry.health_check().await;

                    if !is_healthy && matches!(*entry.state.read().await, ConnectionState::Connected) {
                        // 如果之前是连接状态但现在不健康，尝试重新连接
                        if entry.config.auto_reconnect {
                            if let Err(e) = entry.reconnect().await {
                                error!("Failed to reconnect to {}: {e}", name);
                            }
                        }
                    }
                }
            }
        });
    }

    pub async fn get_connection_state(&self, server_name: &str) -> Option<ConnectionState> {
        if let Some(entry) = self.servers.get(server_name) {
            Some(entry.state.read().await.clone())
        } else {
            None
        }
    }

    pub async fn get_all_states(&self) -> HashMap<String, ConnectionState> {
        let mut states = HashMap::new();
        for (name, entry) in &self.servers {
            states.insert(name.clone(), entry.state.read().await.clone());
        }
        states
    }
}