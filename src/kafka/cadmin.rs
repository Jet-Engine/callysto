use crate::kafka::contexts::CConsumerContext;
use rdkafka::admin::AdminClient;
use rdkafka::ClientConfig;

pub struct CAdminClient {
    admin_client: AdminClient<CConsumerContext>,
    pub consumer_context: CConsumerContext,
    pub client_config: ClientConfig,
}

impl CAdminClient {
    pub fn new(client_config: ClientConfig, consumer_context: CConsumerContext) -> Self {
        let admin_client: AdminClient<CConsumerContext> = client_config
            .create_with_context(consumer_context.clone())
            .expect("Admin client creation failed");

        CAdminClient {
            admin_client,
            consumer_context,
            client_config,
        }
    }
}
