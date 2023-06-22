CREATE
    SCHEMA POSTGRES_FULL;

CREATE
    TYPE mood AS ENUM(
        'sad',
        'ok',
        'happy'
    );

CREATE
    TYPE inventory_item AS(
        name text,
        supplier_id INTEGER,
        price NUMERIC
    );
SET
lc_monetary TO 'en_US.utf8';
SET
TIMEZONE TO 'MST';

CREATE
    EXTENSION hstore;

CREATE
    TABLE
        POSTGRES_FULL.TEST_DATASET(
            id INTEGER PRIMARY KEY,
            test_column_1 BIGINT,
            test_column_10 bytea,
            test_column_11 CHAR,
            test_column_12 CHAR(8),
            test_column_13 CHARACTER,
            test_column_14 CHARACTER(8),
            test_column_15 text,
            test_column_16 VARCHAR,
            test_column_17 CHARACTER VARYING(10),
            test_column_18 cidr,
            test_column_19 circle,
            test_column_2 bigserial,
            test_column_20 DATE NOT NULL DEFAULT now(),
            test_column_21 DATE,
            test_column_22 float8,
            test_column_23 FLOAT,
            test_column_24 DOUBLE PRECISION,
            test_column_25 inet,
            test_column_26 int4,
            test_column_27 INT,
            test_column_28 INTEGER,
            test_column_29 INTERVAL,
            test_column_3 BIT(1),
            test_column_30 json,
            test_column_31 jsonb,
            test_column_32 line,
            test_column_33 lseg,
            test_column_34 macaddr,
            test_column_35 macaddr8,
            test_column_36 money,
            test_column_37 DECIMAL,
            test_column_38 NUMERIC,
            test_column_39 PATH,
            test_column_4 BIT(3),
            test_column_40 pg_lsn,
            test_column_41 point,
            test_column_42 polygon,
            test_column_43 float4,
            test_column_44 REAL,
            test_column_45 int2,
            test_column_46 SMALLINT,
            test_column_47 serial2,
            test_column_48 smallserial,
            test_column_49 serial4,
            test_column_5 BIT VARYING(5),
            test_column_51 TIME WITHOUT TIME ZONE,
            test_column_52 TIME,
            test_column_53 TIME WITHOUT TIME ZONE NOT NULL DEFAULT now(),
            test_column_54 TIMESTAMP,
            test_column_55 TIMESTAMP WITHOUT TIME ZONE,
            test_column_56 TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
            test_column_57 TIMESTAMP,
            test_column_58 TIMESTAMP WITHOUT TIME ZONE,
            test_column_59 TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
            test_column_6 BIT VARYING(5),
            test_column_60 TIMESTAMP WITH TIME ZONE,
            test_column_61 timestamptz,
            test_column_62 tsquery,
            test_column_63 tsvector,
            test_column_64 uuid,
            test_column_65 xml,
            test_column_66 mood,
            test_column_67 tsrange,
            test_column_68 inventory_item,
            test_column_69 hstore,
            test_column_7 bool,
            test_column_70 TIME WITH TIME ZONE,
            test_column_71 timetz,
            test_column_72 INT2 [],
            test_column_73 INT4 [],
            test_column_74 INT8 [],
            test_column_75 OID [],
            test_column_76 VARCHAR [],
            test_column_77 CHAR(1)[],
            test_column_78 BPCHAR(2)[],
            test_column_79 TEXT [],
            test_column_8 BOOLEAN,
            test_column_80 NAME [],
            test_column_81 NUMERIC [],
            test_column_82 DECIMAL [],
            test_column_83 FLOAT4 [],
            test_column_84 FLOAT8 [],
            test_column_85 MONEY [],
            test_column_86 BOOL [],
            test_column_87 BIT [],
            test_column_88 BYTEA [],
            test_column_89 DATE [],
            test_column_9 box,
            test_column_90 TIME(6)[],
            test_column_91 TIMETZ [],
            test_column_92 TIMESTAMPTZ [],
            test_column_93 TIMESTAMP []
        );

INSERT
    INTO
        POSTGRES_FULL.TEST_DATASET
    VALUES(
        1,
        - 9223372036854775808,
        NULL,
        'a',
        '{asb123}',
        'a',
        '{asb123}',
        'a',
        'a',
        '{asb123}',
        NULL,
        '(5,7),10',
        1,
        '1999-01-08',
        '1999-01-08',
        '123',
        '123',
        '123',
        '198.24.10.0/24',
        NULL,
        NULL,
        NULL,
        NULL,
        B'0',
        NULL,
        NULL,
        '{4,5,6}',
        '((3,7),(15,18))',
        NULL,
        NULL,
        NULL,
        '123',
        '123',
        '((3,7),(15,18))',
        B'101',
        '7/A25801C8'::pg_lsn,
        '(3,7)',
        '((3,7),(15,18))',
        NULL,
        NULL,
        NULL,
        NULL,
        1,
        1,
        1,
        B'101',
        '13:00:01',
        '13:00:01',
        '13:00:01',
        TIMESTAMP '2004-10-19 10:23:00',
        TIMESTAMP '2004-10-19 10:23:00',
        TIMESTAMP '2004-10-19 10:23:00',
        'infinity',
        'infinity',
        'infinity',
        B'101',
        TIMESTAMP WITH TIME ZONE '2004-10-19 10:23:00-08',
        TIMESTAMP WITH TIME ZONE '2004-10-19 10:23:00-08',
        NULL,
        to_tsvector('The quick brown fox jumped over the lazy dog.'),
        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
        XMLPARSE(
            DOCUMENT '<?xml version="1.0"?><book><title>Manual</title><chapter>...</chapter></book>'
        ),
        'happy',
        '(2010-01-01 14:30, 2010-01-01 15:30)',
        ROW(
            'fuzzy dice',
            42,
            1.99
        ),
        '"paperback" => "243","publisher" => "postgresqltutorial.com",
"language"  => "English","ISBN-13" => "978-1449370000",
"weight"    => "11.2 ounces"',
        TRUE,
        NULL,
        NULL,
        '{1,2,3}',
        '{-2147483648,2147483646}',
        '{-9223372036854775808,9223372036854775801}',
        '{564182,234181}',
        '{lorem ipsum,dolor sit,amet}',
        '{l,d,a}',
        '{l,d,a}',
        '{someeeeee loooooooooong teeeeext,vvvvvvveeeeeeeeeeeruyyyyyyyyy looooooooooooooooong teeeeeeeeeeeeeeext}',
        TRUE,
        '{object,integer}',
        '{131070.23,231072.476596593}',
        '{131070.23,231072.476596593}',
        '{131070.237689,231072.476596593}',
        '{131070.237689,231072.476596593}',
        '{$999.99,$1001.01,45000, $1.001,$800,22222.006, 1001.01}',
        '{true,yes,1,false,no,0,null}',
        '{null,1,0}',
        '{\xA6697E974E6A320F454390BE03F74955E8978F1A6971EA6730542E37B66179BC,\x4B52414B00000000000000000000000000000000000000000000000000000000}',
        '{1999-01-08,1991-02-10 BC}',
        '((3,7),(15,18))',
        '{13:00:01,13:00:02+8,13:00:03-8,13:00:04Z,13:00:05.000000+8,13:00:00Z-8}',
        '{null,13:00:01,13:00:00+8,13:00:03-8,13:00:04Z,13:00:05.012345Z+8,13:00:06.00000Z-8,13:00}',
        '{null,2004-10-19 10:23:00-08,2004-10-19 10:23:54.123456-08}',
        '{null,2004-10-19 10:23:00,2004-10-19 10:23:54.123456,3004-10-19 10:23:54.123456 BC}'
    );

INSERT
    INTO
        POSTGRES_FULL.TEST_DATASET
    VALUES(
        2,
        9223372036854775807,
        decode(
            '1234',
            'hex'
        ),
        '*',
        '{asb12}',
        '*',
        '{asb12}',
        'abc',
        'abc',
        '{asb12}',
        '192.168.100.128/25',
        '(0,0),0',
        9223372036854775807,
        '1991-02-10 BC',
        '1991-02-10 BC',
        '1234567890.1234567',
        '1234567890.1234567',
        '1234567890.1234567',
        '198.24.10.0',
        1001,
        1001,
        1001,
        'P1Y2M3DT4H5M6S',
        NULL,
        '{"a": 10, "b": 15}',
        '[1, 2, 3]'::jsonb,
        '{0,1,0}',
        '((0,0),(0,0))',
        '08:00:2b:01:02:03',
        '08:00:2b:01:02:03:04:05',
        '999.99',
        NULL,
        NULL,
        '((0,0),(0,0))',
        NULL,
        '0/0'::pg_lsn,
        '(0,0)',
        '((0,0),(0,0))',
        3.4145,
        3.4145,
        - 32768,
        - 32768,
        32767,
        32767,
        2147483647,
        NULL,
        '13:00:02+8',
        '13:00:02+8',
        '13:00:02+8',
        TIMESTAMP '2004-10-19 10:23:54.123456',
        TIMESTAMP '2004-10-19 10:23:54.123456',
        TIMESTAMP '2004-10-19 10:23:54.123456',
        '-infinity',
        '-infinity',
        '-infinity',
        NULL,
        TIMESTAMP WITH TIME ZONE '2004-10-19 10:23:54.123456-08',
        TIMESTAMP WITH TIME ZONE '2004-10-19 10:23:54.123456-08',
        'fat & (rat | cat)'::tsquery,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        'yes',
        '13:00:01',
        '13:00:01',
        '{4,5,6}',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        'yes',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '((0,0),(0,0))',
        NULL,
        NULL,
        NULL,
        NULL
    );

INSERT
    INTO
        POSTGRES_FULL.TEST_DATASET
    VALUES(
        3,
        0,
        '1234',
        NULL,
        NULL,
        NULL,
        NULL,
        'Миші йдуть на південь, не питай чому;',
        'Миші йдуть на південь, не питай чому;',
        NULL,
        '192.168/24',
        '(-10,-4),10',
        0,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '198.10/8',
        - 2147483648,
        - 2147483648,
        - 2147483648,
        '-178000000',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '08-00-2b-01-02-04',
        '08-00-2b-01-02-03-04-06',
        '1,001.01',
        '1234567890.1234567',
        '1234567890.1234567',
        NULL,
        NULL,
        NULL,
        '(999999999999999999999999,0)',
        '((0,0),(999999999999999999999999,0))',
        NULL,
        NULL,
        32767,
        32767,
        0,
        0,
        0,
        NULL,
        '13:00:03-8',
        '13:00:03-8',
        '13:00:03-8',
        TIMESTAMP '3004-10-19 10:23:54.123456 BC',
        TIMESTAMP '3004-10-19 10:23:54.123456 BC',
        TIMESTAMP '3004-10-19 10:23:54.123456 BC',
        NULL,
        NULL,
        NULL,
        NULL,
        TIMESTAMP WITH TIME ZONE '3004-10-19 10:23:54.123456-08 BC',
        TIMESTAMP WITH TIME ZONE '3004-10-19 10:23:54.123456-08 BC',
        'fat:ab & cat'::tsquery,
        NULL,
        NULL,
        '',
        NULL,
        NULL,
        NULL,
        NULL,
        '1',
        '13:00:00+8',
        '13:00:00+8',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '1',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    );

INSERT
    INTO
        POSTGRES_FULL.TEST_DATASET
    VALUES(
        4,
        NULL,
        'abcd',
        NULL,
        NULL,
        NULL,
        NULL,
        '櫻花分店',
        '櫻花分店',
        NULL,
        '192.168.1',
        NULL,
        - 9223372036854775808,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        2147483647,
        2147483647,
        2147483647,
        '178000000',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '08002b:010205',
        '08002b:0102030407',
        '-1,000',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        - 32767,
        - 32767,
        - 2147483647,
        NULL,
        '13:00:04Z',
        '13:00:04Z',
        '13:00:04Z',
        TIMESTAMP '0001-01-01 00:00:00.000000',
        TIMESTAMP '0001-01-01 00:00:00.000000',
        TIMESTAMP '0001-01-01 00:00:00.000000',
        NULL,
        NULL,
        NULL,
        NULL,
        TIMESTAMP WITH TIME ZONE '0001-12-31 16:00:00.000000-08 BC',
        TIMESTAMP WITH TIME ZONE '0001-12-31 16:00:00.000000-08 BC',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        FALSE,
        '13:00:03-8',
        '13:00:03-8',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        FALSE,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    );

INSERT
    INTO
        POSTGRES_FULL.TEST_DATASET
    VALUES(
        5,
        NULL,
        '\xabcd',
        NULL,
        NULL,
        NULL,
        NULL,
        '',
        '',
        NULL,
        '128.1',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '$999.99',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '13:00:05.01234Z+8',
        '13:00:05.01234Z+8',
        '13:00:05.01234Z+8',
        TIMESTAMP '0001-12-31 23:59:59.999999 BC',
        TIMESTAMP '0001-12-31 23:59:59.999999 BC',
        TIMESTAMP '0001-12-31 23:59:59.999999 BC',
        NULL,
        NULL,
        NULL,
        NULL,
        TIMESTAMP WITH TIME ZONE '0001-12-31 15:59:59.999999-08 BC',
        TIMESTAMP WITH TIME ZONE '0001-12-31 15:59:59.999999-08 BC',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        'no',
        '13:00:04Z',
        '13:00:04Z',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        'no',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    );

INSERT
    INTO
        POSTGRES_FULL.TEST_DATASET
    VALUES(
        6,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '2001:4f8:3:ba::/64',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '$1001.01',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '13:00:00Z-8',
        '13:00:00Z-8',
        '13:00:00Z-8',
        'epoch',
        'epoch',
        'epoch',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '0',
        '13:00:05.012345Z+8',
        '13:00:05.012345Z+8',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '0',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    );

INSERT
    INTO
        POSTGRES_FULL.TEST_DATASET
    VALUES(
        7,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '\xF0\x9F\x9A\x80',
        '\xF0\x9F\x9A\x80',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '-$1,000',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '24:00:00',
        '24:00:00',
        '24:00:00',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '13:00:06.00000Z-8',
        '13:00:06.00000Z-8',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    );
