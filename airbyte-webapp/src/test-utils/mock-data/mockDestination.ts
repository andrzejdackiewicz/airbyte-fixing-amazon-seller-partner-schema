import { DestinationDefinitionSpecificationRead, DestinationDefinitionRead } from "core/request/AirbyteClient";
import { ConnectorIds } from "utils/connectors";

export const mockDestinationDefinition: DestinationDefinitionRead = {
  destinationDefinitionId: ConnectorIds.Destinations.Postgres,
  name: "Postgres",
  dockerRepository: "airbyte/destination-postgres",
  dockerImageTag: "0.3.26",
  documentationUrl: "https://docs.airbyte.com/integrations/destinations/postgres",
  icon: '<svg xmlns="http://www.w3.org/2000/svg" xml:space="preserve" width="576.095" height="593.844" viewBox="0 0 432.071 445.383"><g style="fill-rule:nonzero;clip-rule:nonzero;fill:none;stroke:#fff;stroke-width:12.4651;stroke-linecap:round;stroke-linejoin:round;stroke-miterlimit:4"><path d="M323.205 324.227c2.833-23.601 1.984-27.062 19.563-23.239l4.463.392c13.517.615 31.199-2.174 41.587-7 22.362-10.376 35.622-27.7 13.572-23.148-50.297 10.376-53.755-6.655-53.755-6.655 53.111-78.803 75.313-178.836 56.149-203.322-52.27-66.789-142.748-35.206-144.262-34.386l-.482.089c-9.938-2.062-21.06-3.294-33.554-3.496-22.761-.374-40.032 5.967-53.133 15.904 0 0-161.408-66.498-153.899 83.628 1.597 31.936 45.777 241.655 98.47 178.31 19.259-23.163 37.871-42.748 37.871-42.748 9.242 6.14 20.307 9.272 31.912 8.147l.897-.765c-.281 2.876-.157 5.689.359 9.019-13.572 15.167-9.584 17.83-36.723 23.416-27.457 5.659-11.326 15.734-.797 18.367 12.768 3.193 42.305 7.716 62.268-20.224l-.795 3.188c5.325 4.26 4.965 30.619 5.72 49.452.756 18.834 2.017 36.409 5.856 46.771 3.839 10.36 8.369 37.05 44.036 29.406 29.809-6.388 52.6-15.582 54.677-101.107" style="fill:#000;stroke:#000;stroke-width:37.3953;stroke-linecap:butt;stroke-linejoin:miter"/><path stroke="none" d="M402.395 271.23c-50.302 10.376-53.76-6.655-53.76-6.655 53.111-78.808 75.313-178.843 56.153-203.326-52.27-66.785-142.752-35.2-144.262-34.38l-.486.087c-9.938-2.063-21.06-3.292-33.56-3.496-22.761-.373-40.026 5.967-53.127 15.902 0 0-161.411-66.495-153.904 83.63 1.597 31.938 45.776 241.657 98.471 178.312 19.26-23.163 37.869-42.748 37.869-42.748 9.243 6.14 20.308 9.272 31.908 8.147l.901-.765c-.28 2.876-.152 5.689.361 9.019-13.575 15.167-9.586 17.83-36.723 23.416-27.459 5.659-11.328 15.734-.796 18.367 12.768 3.193 42.307 7.716 62.266-20.224l-.796 3.188c5.319 4.26 9.054 27.711 8.428 48.969-.626 21.259-1.044 35.854 3.147 47.254 4.191 11.4 8.368 37.05 44.042 29.406 29.809-6.388 45.256-22.942 47.405-50.555 1.525-19.631 4.976-16.729 5.194-34.28l2.768-8.309c3.192-26.611.507-35.196 18.872-31.203l4.463.392c13.517.615 31.208-2.174 41.591-7 22.358-10.376 35.618-27.7 13.573-23.148z" style="fill:#336791;stroke:none"/><path d="M215.866 286.484c-1.385 49.516.348 99.377 5.193 111.495 4.848 12.118 15.223 35.688 50.9 28.045 29.806-6.39 40.651-18.756 45.357-46.051 3.466-20.082 10.148-75.854 11.005-87.281M173.104 38.256S11.583-27.76 19.092 122.365c1.597 31.938 45.779 241.664 98.473 178.316 19.256-23.166 36.671-41.335 36.671-41.335M260.349 26.207c-5.591 1.753 89.848-34.889 144.087 34.417 19.159 24.484-3.043 124.519-56.153 203.329"/><path d="M348.282 263.953s3.461 17.036 53.764 6.653c22.04-4.552 8.776 12.774-13.577 23.155-18.345 8.514-59.474 10.696-60.146-1.069-1.729-30.355 21.647-21.133 19.96-28.739-1.525-6.85-11.979-13.573-18.894-30.338-6.037-14.633-82.796-126.849 21.287-110.183 3.813-.789-27.146-99.002-124.553-100.599-97.385-1.597-94.19 119.762-94.19 119.762" style="stroke-linejoin:bevel"/><path d="M188.604 274.334c-13.577 15.166-9.584 17.829-36.723 23.417-27.459 5.66-11.326 15.733-.797 18.365 12.768 3.195 42.307 7.718 62.266-20.229 6.078-8.509-.036-22.086-8.385-25.547-4.034-1.671-9.428-3.765-16.361 3.994z"/><path d="M187.715 274.069c-1.368-8.917 2.93-19.528 7.536-31.942 6.922-18.626 22.893-37.255 10.117-96.339-9.523-44.029-73.396-9.163-73.436-3.193-.039 5.968 2.889 30.26-1.067 58.548-5.162 36.913 23.488 68.132 56.479 64.938"/><path d="M172.517 141.7c-.288 2.039 3.733 7.48 8.976 8.207 5.234.73 9.714-3.522 9.998-5.559.284-2.039-3.732-4.285-8.977-5.015-5.237-.731-9.719.333-9.996 2.367z" style="fill:#fff;stroke-width:4.155;stroke-linecap:butt;stroke-linejoin:miter"/><path d="M331.941 137.543c.284 2.039-3.732 7.48-8.976 8.207-5.238.73-9.718-3.522-10.005-5.559-.277-2.039 3.74-4.285 8.979-5.015 5.239-.73 9.718.333 10.002 2.368z" style="fill:#fff;stroke-width:2.0775;stroke-linecap:butt;stroke-linejoin:miter"/><path d="M350.676 123.432c.863 15.994-3.445 26.888-3.988 43.914-.804 24.748 11.799 53.074-7.191 81.435"/></g></svg>',
  releaseStage: "alpha",
  supportsDbt: true,
  normalizationConfig: {
    normalizationRepository: "airbyte/normalization",
    normalizationTag: "0.2.25",
    normalizationIntegrationType: "postgres",
  },
};

export const mockDestinationDefinitionSpecification: DestinationDefinitionSpecificationRead = {
  destinationDefinitionId: ConnectorIds.Destinations.Postgres,
  documentationUrl: "https://docs.airbyte.io/integrations/destinations/postgres",
  connectionSpecification: {
    type: "object",
    title: "Postgres Destination Spec",
    $schema: "http://json-schema.org/draft-07/schema#",
    required: ["host", "port", "username", "database", "schema"],
    properties: {
      ssl: {
        type: "boolean",
        order: 6,
        title: "SSL Connection",
        default: false,
        description: "Encrypt data using SSL. When activating SSL, please select one of the connection modes.",
      },
      host: {
        type: "string",
        order: 0,
        title: "Host",
        description: "Hostname of the database.",
      },
      port: {
        type: "integer",
        order: 1,
        title: "Port",
        default: 5432,
        maximum: 65536,
        minimum: 0,
        examples: ["5432"],
        description: "Port of the database.",
      },
      schema: {
        type: "string",
        order: 3,
        title: "Default Schema",
        default: "public",
        examples: ["public"],
        description:
          'The default schema tables are written to if the source does not specify a namespace. The usual value for this field is "public".',
      },
      database: {
        type: "string",
        order: 2,
        title: "DB Name",
        description: "Name of the database.",
      },
      password: {
        type: "string",
        order: 5,
        title: "Password",
        description: "Password associated with the username.",
        airbyte_secret: true,
      },
      ssl_mode: {
        type: "object",
        oneOf: [
          {
            title: "disable",
            required: ["mode"],
            properties: {
              mode: {
                enum: ["disable"],
                type: "string",
                const: "disable",
                order: 0,
                default: "disable",
              },
            },
            description: "Disable SSL.",
            additionalProperties: false,
          },
          {
            title: "allow",
            required: ["mode"],
            properties: {
              mode: {
                enum: ["allow"],
                type: "string",
                const: "allow",
                order: 0,
                default: "allow",
              },
            },
            description: "Allow SSL mode.",
            additionalProperties: false,
          },
          {
            title: "prefer",
            required: ["mode"],
            properties: {
              mode: {
                enum: ["prefer"],
                type: "string",
                const: "prefer",
                order: 0,
                default: "prefer",
              },
            },
            description: "Prefer SSL mode.",
            additionalProperties: false,
          },
          {
            title: "require",
            required: ["mode"],
            properties: {
              mode: {
                enum: ["require"],
                type: "string",
                const: "require",
                order: 0,
                default: "require",
              },
            },
            description: "Require SSL mode.",
            additionalProperties: false,
          },
          {
            title: "verify-ca",
            required: ["mode", "ca_certificate"],
            properties: {
              mode: {
                enum: ["verify-ca"],
                type: "string",
                const: "verify-ca",
                order: 0,
                default: "verify-ca",
              },
              ca_certificate: {
                type: "string",
                order: 1,
                title: "CA certificate",
                multiline: true,
                description: "CA certificate",
                airbyte_secret: true,
              },
              client_key_password: {
                type: "string",
                order: 4,
                title: "Client key password (Optional)",
                description:
                  "Password for keystorage. This field is optional. If you do not add it - the password will be generated automatically.",
                airbyte_secret: true,
              },
            },
            description: "Verify-ca SSL mode.",
            additionalProperties: false,
          },
          {
            title: "verify-full",
            required: ["mode", "ca_certificate", "client_certificate", "client_key"],
            properties: {
              mode: {
                enum: ["verify-full"],
                type: "string",
                const: "verify-full",
                order: 0,
                default: "verify-full",
              },
              client_key: {
                type: "string",
                order: 3,
                title: "Client key",
                multiline: true,
                description: "Client key",
                airbyte_secret: true,
              },
              ca_certificate: {
                type: "string",
                order: 1,
                title: "CA certificate",
                multiline: true,
                description: "CA certificate",
                airbyte_secret: true,
              },
              client_certificate: {
                type: "string",
                order: 2,
                title: "Client certificate",
                multiline: true,
                description: "Client certificate",
                airbyte_secret: true,
              },
              client_key_password: {
                type: "string",
                order: 4,
                title: "Client key password (Optional)",
                description:
                  "Password for keystorage. This field is optional. If you do not add it - the password will be generated automatically.",
                airbyte_secret: true,
              },
            },
            description: "Verify-full SSL mode.",
            additionalProperties: false,
          },
        ],
        order: 7,
        title: "SSL modes",
        description:
          'SSL connection modes. \n <b>disable</b> - Chose this mode to disable encryption of communication between Airbyte and destination database\n <b>allow</b> - Chose this mode to enable encryption only when required by the source database\n <b>prefer</b> - Chose this mode to allow unencrypted connection only if the source database does not support encryption\n <b>require</b> - Chose this mode to always require encryption. If the source database server does not support encryption, connection will fail\n  <b>verify-ca</b> - Chose this mode to always require encryption and to verify that the source database server has a valid SSL certificate\n  <b>verify-full</b> - This is the most secure mode. Chose this mode to always require encryption and to verify the identity of the source database server\n See more information - <a href="https://jdbc.postgresql.org/documentation/head/ssl-client.html"> in the docs</a>.',
      },
      username: {
        type: "string",
        order: 4,
        title: "User",
        description: "Username to use to access the database.",
      },
      tunnel_method: {
        type: "object",
        oneOf: [
          {
            title: "No Tunnel",
            required: ["tunnel_method"],
            properties: {
              tunnel_method: {
                type: "string",
                const: "NO_TUNNEL",
                order: 0,
                description: "No ssh tunnel needed to connect to database",
              },
            },
          },
          {
            title: "SSH Key Authentication",
            required: ["tunnel_method", "tunnel_host", "tunnel_port", "tunnel_user", "ssh_key"],
            properties: {
              ssh_key: {
                type: "string",
                order: 4,
                title: "SSH Private Key",
                multiline: true,
                description:
                  "OS-level user account ssh key credentials in RSA PEM format ( created with ssh-keygen -t rsa -m PEM -f myuser_rsa )",
                airbyte_secret: true,
              },
              tunnel_host: {
                type: "string",
                order: 1,
                title: "SSH Tunnel Jump Server Host",
                description: "Hostname of the jump server host that allows inbound ssh tunnel.",
              },
              tunnel_port: {
                type: "integer",
                order: 2,
                title: "SSH Connection Port",
                default: 22,
                maximum: 65536,
                minimum: 0,
                examples: ["22"],
                description: "Port on the proxy/jump server that accepts inbound ssh connections.",
              },
              tunnel_user: {
                type: "string",
                order: 3,
                title: "SSH Login Username",
                description: "OS-level username for logging into the jump server host.",
              },
              tunnel_method: {
                type: "string",
                const: "SSH_KEY_AUTH",
                order: 0,
                description: "Connect through a jump server tunnel host using username and ssh key",
              },
            },
          },
          {
            title: "Password Authentication",
            required: ["tunnel_method", "tunnel_host", "tunnel_port", "tunnel_user", "tunnel_user_password"],
            properties: {
              tunnel_host: {
                type: "string",
                order: 1,
                title: "SSH Tunnel Jump Server Host",
                description: "Hostname of the jump server host that allows inbound ssh tunnel.",
              },
              tunnel_port: {
                type: "integer",
                order: 2,
                title: "SSH Connection Port",
                default: 22,
                maximum: 65536,
                minimum: 0,
                examples: ["22"],
                description: "Port on the proxy/jump server that accepts inbound ssh connections.",
              },
              tunnel_user: {
                type: "string",
                order: 3,
                title: "SSH Login Username",
                description: "OS-level username for logging into the jump server host",
              },
              tunnel_method: {
                type: "string",
                const: "SSH_PASSWORD_AUTH",
                order: 0,
                description: "Connect through a jump server tunnel host using username and password authentication",
              },
              tunnel_user_password: {
                type: "string",
                order: 4,
                title: "Password",
                description: "OS-level password for logging into the jump server host",
                airbyte_secret: true,
              },
            },
          },
        ],
        title: "SSH Tunnel Method",
        description:
          "Whether to initiate an SSH tunnel before connecting to the database, and if so, which kind of authentication to use.",
      },
      jdbc_url_params: {
        type: "string",
        order: 8,
        title: "JDBC URL Params",
        description:
          "Additional properties to pass to the JDBC URL string when connecting to the database formatted as 'key=value' pairs separated by the symbol '&'. (example: key1=value1&key2=value2&key3=value3).",
      },
    },
    additionalProperties: true,
  },
  jobInfo: {
    id: "7c3ae799-cb25-4c05-9685-f7bb1885d662",
    configType: "get_spec",
    configId: "Optional.empty",
    createdAt: 1661365436880,
    endedAt: 1661365436880,
    succeeded: true,
    logs: {
      logLines: [],
    },
  },
  supportedDestinationSyncModes: ["overwrite", "append", "append_dedup"],
};
