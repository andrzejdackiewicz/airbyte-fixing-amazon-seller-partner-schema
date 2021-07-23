-- types
 CREATE
    TYPE JOB_STATUS AS ENUM(
        'pending',
        'running',
        'incomplete',
        'failed',
        'succeeded',
        'cancelled'
    );

CREATE
    TYPE ATTEMPT_STATUS AS ENUM(
        'running',
        'failed',
        'succeeded'
    );

CREATE
    TYPE JOB_CONFIG_TYPE AS ENUM(
        'check_connection_source',
        'check_connection_destination',
        'discover_schema',
        'get_spec',
        'sync',
        'reset_connection'
    );

-- tables
 CREATE
    TABLE
        AIRBYTE_METADATA(
            KEY VARCHAR(255) PRIMARY KEY,
            value VARCHAR(255)
        );

CREATE
    TABLE
        JOBS(
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            config_type JOB_CONFIG_TYPE,
            SCOPE VARCHAR(255),
            config JSONB,
            status JOB_STATUS,
            started_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ
        );

CREATE
    TABLE
        ATTEMPTS(
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            job_id BIGINT,
            attempt_number INTEGER,
            log_path VARCHAR(255),
            OUTPUT JSONB,
            status ATTEMPT_STATUS,
            created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ,
            ended_at TIMESTAMPTZ
        );

CREATE
    UNIQUE INDEX IF NOT EXISTS job_attempt_idx ON
    ATTEMPTS(
        job_id,
        attempt_number
    );

-- entries
 INSERT
    INTO
        AIRBYTE_METADATA
    VALUES(
        'server_uuid',
        '__uuid__'
    );
