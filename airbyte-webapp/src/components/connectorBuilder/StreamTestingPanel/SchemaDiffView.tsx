import { faWarning } from "@fortawesome/free-solid-svg-icons";
import classNames from "classnames";
import { diffJson, Change } from "diff";
import { useField } from "formik";
import merge from "lodash/merge";
import React, { useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useDebounce } from "react-use";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { InfoBox } from "components/ui/InfoBox";

import { StreamReadInferredSchema } from "core/request/ConnectorBuilderClient";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderTestState,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import { formatJson } from "../utils";
import styles from "./SchemaDiffView.module.scss";

interface SchemaDiffViewProps {
  inferredSchema: StreamReadInferredSchema;
}

interface Diff {
  /**
   * List of changes from current schema to detected schema
   */
  changes: Change[];
  /**
   * Formatted merged schema if merging in the detected schema changes the existing schema
   */
  mergedSchema?: string;
  /**
   * Flag if overriding the existing schema with the new schema would lose information
   */
  lossyOverride: boolean;
}

function getDiff(existingSchema: string | undefined, detectedSchema: object): Diff {
  if (!existingSchema) {
    return { changes: [], lossyOverride: false };
  }
  try {
    const existingObject = existingSchema ? JSON.parse(existingSchema) : undefined;
    const mergedSchemaPreferExisting = formatJson(merge({}, detectedSchema, existingObject));
    const changes = diffJson(existingObject, detectedSchema);
    // The override would be lossy if lines are removed in the diff
    const lossyOverride = changes.some((change) => change.removed);
    return {
      changes,
      mergedSchema: mergedSchemaPreferExisting !== existingSchema ? mergedSchemaPreferExisting : undefined,
      lossyOverride,
    };
  } catch {
    return { changes: [], lossyOverride: true };
  }
}

export const SchemaDiffView: React.FC<SchemaDiffViewProps> = ({ inferredSchema }) => {
  const { testStreamIndex } = useConnectorBuilderTestState();
  const { editorView } = useConnectorBuilderFormState();
  const [field, , helpers] = useField(`streams[${testStreamIndex}].schema`);
  const formattedSchema = useMemo(() => inferredSchema && formatJson(inferredSchema), [inferredSchema]);

  const [schemaDiff, setSchemaDiff] = useState<Diff>(() =>
    editorView === "ui" ? getDiff(field.value, inferredSchema) : { changes: [], lossyOverride: false }
  );

  useDebounce(
    () => {
      if (editorView === "ui") {
        setSchemaDiff(getDiff(field.value, inferredSchema));
      }
    },
    250,
    [field.value, inferredSchema, editorView]
  );

  return (
    <FlexContainer direction="column">
      {editorView === "ui" && field.value && field.value !== formattedSchema && (
        <InfoBox icon={faWarning} className={styles.infoBox}>
          <FlexItem grow>
            <FlexContainer direction="column">
              <FormattedMessage id="connectorBuilder.differentSchemaDescription" />
              <FlexContainer>
                <FlexItem grow>
                  <Button
                    full
                    variant="dark"
                    disabled={field.value === formattedSchema}
                    onClick={() => {
                      helpers.setValue(formattedSchema);
                    }}
                  >
                    <FormattedMessage
                      id={
                        schemaDiff.lossyOverride
                          ? "connectorBuilder.overwriteSchemaButton"
                          : "connectorBuilder.useSchemaButton"
                      }
                    />
                  </Button>
                </FlexItem>
                {schemaDiff.mergedSchema && schemaDiff.lossyOverride && (
                  <FlexItem grow>
                    <Button
                      full
                      variant="dark"
                      onClick={() => {
                        helpers.setValue(schemaDiff.mergedSchema);
                      }}
                    >
                      <FormattedMessage id="connectorBuilder.mergeSchemaButton" />
                    </Button>
                  </FlexItem>
                )}
              </FlexContainer>
            </FlexContainer>
          </FlexItem>
        </InfoBox>
      )}
      {!field.value && (
        <Button
          full
          variant="secondary"
          onClick={() => {
            helpers.setValue(formattedSchema);
          }}
        >
          <FormattedMessage id="connectorBuilder.useSchemaButton" />
        </Button>
      )}
      <FlexItem>
        {!schemaDiff.changes.length ? (
          <pre className={styles.diffLine}>
            {formattedSchema
              .split("\n")
              .map((line) => ` ${line}`)
              .join("\n")}
          </pre>
        ) : (
          schemaDiff.changes.map((change, changeIndex) => (
            <pre
              className={classNames(
                {
                  [styles.added]: change.added,
                  [styles.removed]: change.removed,
                },
                styles.diffLine
              )}
              key={changeIndex}
            >
              {change.value
                .split("\n")
                .map((line) => (line === "" ? undefined : `${change.added ? "+" : change.removed ? "-" : " "}${line}`))
                .filter(Boolean)
                .join("\n")}
            </pre>
          ))
        )}
      </FlexItem>
    </FlexContainer>
  );
};
