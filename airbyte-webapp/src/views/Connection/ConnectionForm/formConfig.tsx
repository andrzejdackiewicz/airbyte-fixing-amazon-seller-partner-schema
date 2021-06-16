import { useMemo } from "react";
import { useIntl } from "react-intl";
import * as yup from "yup";

import {
  AirbyteStreamConfiguration,
  DestinationSyncMode,
  SyncMode,
  SyncSchema,
  SyncSchemaStream,
} from "core/domain/catalog";
import { ValuesProps } from "components/hooks/services/useConnectionHook";
import {
  Normalization,
  NormalizationType,
  Operation,
  OperatorType,
  Transformation,
} from "core/domain/connection/operation";
import { DropDownRow } from "components";
import FrequencyConfig from "config/FrequencyConfig.json";
import { DestinationDefinitionSpecification } from "core/resources/DestinationDefinitionSpecification";

type FormikConnectionFormValues = {
  frequency: string;
  prefix: string;
  syncCatalog: SyncSchema;
  transformations?: Transformation[];
  normalization?: NormalizationType;
};

type ConnectionFormValues = ValuesProps;

const SUPPORTED_MODES: [SyncMode, DestinationSyncMode][] = [
  [SyncMode.FullRefresh, DestinationSyncMode.Overwrite],
  [SyncMode.FullRefresh, DestinationSyncMode.Append],
  [SyncMode.Incremental, DestinationSyncMode.Append],
  [SyncMode.Incremental, DestinationSyncMode.Dedupted],
];

const DEFAULT_TRANSFORMATION: Transformation = {
  name: "My dbt transformations",
  operatorConfiguration: {
    operatorType: OperatorType.Dbt,
    dbt: {
      dockerImage: "fishtownanalytics/dbt:0.19.1",
      dbtArguments: "run",
    },
  },
};

const connectionValidationSchema = yup.object<ConnectionFormValues>({
  frequency: yup.string().required("form.empty.error"),
  prefix: yup.string(),
  syncCatalog: yup.object<SyncSchema>({
    streams: yup.array().of(
      yup.object<SyncSchemaStream>({
        id: yup
          .string()
          // This is required to get rid of id fields we are using to detect stream for edition
          .when("$isRequest", (isRequest: boolean, schema: yup.StringSchema) =>
            isRequest ? schema.strip(true) : schema
          ),
        stream: yup.object(),
        // @ts-ignore
        config: yup.object().test({
          name: "connectionSchema.config.validator",
          // eslint-disable-next-line no-template-curly-in-string
          message: "${path} is wrong",
          test: function (value: AirbyteStreamConfiguration) {
            if (!value.selected) {
              return true;
            }
            if (DestinationSyncMode.Dedupted === value.destinationSyncMode) {
              if (value.primaryKey.length === 0) {
                return this.createError({
                  message: "connectionForm.primaryKey.required",
                  path: `schema.streams[${this.parent.id}].config.primaryKey`,
                });
              }
            }

            if (SyncMode.Incremental === value.syncMode) {
              if (
                !this.parent.stream.sourceDefinedCursor &&
                value.cursorField.length === 0
              ) {
                return this.createError({
                  message: "connectionForm.cursorField.required",
                  path: `schema.streams[${this.parent.id}].config.cursorField`,
                });
              }
            }
            return true;
          },
        }),
      })
    ),
  }),
});

/**
 * Returns {@link Operation}[]
 *
 * Maps UI representation of Transformation and Normalization
 * into API's {@link Operation} representation.
 *
 * Always puts normalization as first operation
 * @param values
 * @param initialOperations
 */
function mapFormPropsToOperation(
  values: {
    transformations?: Transformation[];
    normalization?: NormalizationType;
  },
  initialOperations: Operation[] = []
): Operation[] {
  const newOperations: Operation[] = [];

  if (values.normalization) {
    if (values.normalization !== NormalizationType.RAW) {
      const normalizationOperation = initialOperations.find(
        (op) =>
          op.operatorConfiguration.operatorType === OperatorType.Normalization
      );

      if (normalizationOperation) {
        newOperations.push(normalizationOperation);
      } else {
        newOperations.push({
          name: "Normalization",
          operatorConfiguration: {
            operatorType: OperatorType.Normalization,
            normalization: {
              option: values.normalization,
            },
          },
        });
      }
    }
  }

  if (values.transformations) {
    newOperations.push(...values.transformations);
  }

  return newOperations;
}

function getDefaultCursorField(streamNode: SyncSchemaStream): string[] {
  if (streamNode.stream.defaultCursorField.length) {
    return streamNode.stream.defaultCursorField;
  }
  return streamNode.config.cursorField;
}

// If the value in supportedSyncModes is empty assume the only supported sync mode is FULL_REFRESH.
// Otherwise it supports whatever sync modes are present.
const useInitialSchema = (schema: SyncSchema): SyncSchema =>
  useMemo<SyncSchema>(
    () => ({
      streams: schema.streams.map<SyncSchemaStream>((apiNode, id) => {
        const streamNode: SyncSchemaStream = { ...apiNode, id: id.toString() };
        const node = !streamNode.stream.supportedSyncModes?.length
          ? {
              ...streamNode,
              stream: {
                ...streamNode.stream,
                supportedSyncModes: [SyncMode.FullRefresh],
              },
            }
          : streamNode;

        // If syncMode isn't null - don't change item
        if (node.config.syncMode) {
          return node;
        }

        const updateStream = (
          config: Partial<AirbyteStreamConfiguration>
        ): SyncSchemaStream => ({
          ...node,
          config: { ...node.config, ...config },
        });

        const supportedSyncModes = node.stream.supportedSyncModes;

        // If syncMode is null, FULL_REFRESH should be selected by default (if it support FULL_REFRESH).
        if (supportedSyncModes.includes(SyncMode.FullRefresh)) {
          return updateStream({
            syncMode: SyncMode.FullRefresh,
          });
        }

        // If source support INCREMENTAL and not FULL_REFRESH. Set INCREMENTAL
        if (supportedSyncModes.includes(SyncMode.Incremental)) {
          return updateStream({
            cursorField: streamNode.config.cursorField.length
              ? streamNode.config.cursorField
              : getDefaultCursorField(streamNode),
            syncMode: SyncMode.Incremental,
          });
        }

        // If source don't support INCREMENTAL and FULL_REFRESH - set first value from supportedSyncModes list
        return updateStream({
          syncMode: streamNode.stream.supportedSyncModes[0],
        });
      }),
    }),
    [schema.streams]
  );

const useInitialValues = (props: {
  syncCatalog: SyncSchema;
  destDefinition: DestinationDefinitionSpecification;
  operations?: Operation[];
  prefixValue?: string;
  isEditMode?: boolean;
  frequencyValue?: string;
}) => {
  const {
    syncCatalog,
    frequencyValue,
    prefixValue,
    isEditMode,
    destDefinition,
    operations = [],
  } = props;
  const initialSchema = useInitialSchema(syncCatalog);

  return useMemo<FormikConnectionFormValues>(() => {
    const initialValues: FormikConnectionFormValues = {
      syncCatalog: initialSchema,
      frequency: frequencyValue || "",
      prefix: prefixValue || "",
    };

    if (destDefinition.supportsDbt) {
      initialValues.transformations =
        (operations.filter(
          (op) => op.operatorConfiguration.operatorType === OperatorType.Dbt
        ) as Transformation[]) ?? [];
    }

    if (destDefinition.supportsNormalization) {
      let initialNormalization = (operations.find(
        (op) =>
          op.operatorConfiguration.operatorType === OperatorType.Normalization
      ) as Normalization)?.operatorConfiguration?.normalization?.option;

      // If no normalization was selected for already present normalization -> Raw is select
      if (!initialNormalization && isEditMode) {
        initialNormalization = NormalizationType.RAW;
      }

      initialValues.normalization =
        initialNormalization ?? NormalizationType.BASIC;
    }

    return initialValues;
  }, [
    initialSchema,
    frequencyValue,
    prefixValue,
    isEditMode,
    destDefinition,
    operations,
  ]);
};

const useFrequencyDropdownData = (): DropDownRow.IDataItem[] => {
  const formatMessage = useIntl().formatMessage;

  return useMemo(
    () =>
      FrequencyConfig.map((item) => ({
        ...item,
        text:
          item.value === "manual"
            ? item.text
            : formatMessage(
                {
                  id: "form.every",
                },
                {
                  value: item.simpleText || item.text,
                }
              ),
      })),
    [formatMessage]
  );
};

export type { ConnectionFormValues, FormikConnectionFormValues };
export {
  connectionValidationSchema,
  useInitialValues,
  useFrequencyDropdownData,
  mapFormPropsToOperation,
  SUPPORTED_MODES,
  DEFAULT_TRANSFORMATION,
};
