import { JSONSchema7, JSONSchema7Definition } from "json-schema";
import { useMemo } from "react";
import { useIntl } from "react-intl";
import { AnySchema } from "yup";

import { ConnectorDefinitionSpecification, SourceDefinitionSpecificationDraft } from "core/domain/connector";
import { isSourceDefinitionSpecificationDraft } from "core/domain/connector/source";
import { FormBuildError } from "core/form/FormBuildError";
import { jsonSchemaToFormBlock } from "core/form/schemaToFormBlock";
import { buildYupFormForJsonSchema } from "core/form/schemaToYup";
import { FormBlock, FormGroupItem } from "core/form/types";

import { ConnectorFormValues } from "./types";

export interface BuildFormHook {
  initialValues: ConnectorFormValues;
  formFields: FormBlock;
  jsonSchema: JSONSchema7;
}

export function useBuildForm(
  isEditMode: boolean,
  formType: "source" | "destination",
  selectedConnectorDefinitionSpecification: ConnectorDefinitionSpecification | SourceDefinitionSpecificationDraft,
  initialValues?: Partial<ConnectorFormValues>
): BuildFormHook {
  const { formatMessage } = useIntl();

  const isDraft = isSourceDefinitionSpecificationDraft(selectedConnectorDefinitionSpecification);

  const jsonSchema: JSONSchema7 = useMemo(() => {
    const schema: JSONSchema7 = {
      type: "object",
      properties: {
        connectionConfiguration:
          selectedConnectorDefinitionSpecification.connectionSpecification as JSONSchema7Definition,
      },
    };
    if (isDraft) {
      return schema;
    }
    // schema.properties gets defined right above
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    schema.properties!.name = {
      type: "string",
      title: formatMessage({ id: `form.${formType}Name` }),
      description: formatMessage({ id: `form.${formType}Name.message` }),
    };
    schema.required = ["name"];
    return schema;
  }, [formType, formatMessage, isDraft, selectedConnectorDefinitionSpecification.connectionSpecification]);

  const formFields = useMemo<FormBlock>(() => jsonSchemaToFormBlock(jsonSchema), [jsonSchema]);

  if (formFields._type !== "formGroup") {
    throw new FormBuildError("Top level configuration has to be an object");
  }

  const startValues = useMemo<ConnectorFormValues>(() => {
    if (isEditMode || isDraft) {
      return {
        name: "",
        connectionConfiguration: {},
        ...initialValues,
      };
    }
    const baseValues = {
      name: "",
      connectionConfiguration: {},
      ...initialValues,
    };

    function setDefaultValues(formGroup: FormGroupItem, values: Record<string, unknown>) {
      formGroup.properties.forEach((property) => {
        if (property.const) {
          values[property.fieldKey] = property.const;
        }
        if (property.default) {
          values[property.fieldKey] = property.default;
        }
        switch (property._type) {
          case "formGroup":
            values[property.fieldKey] = {};
            setDefaultValues(property, values[property.fieldKey] as Record<string, unknown>);
            break;
          case "formCondition":
            // implicitly select the first option (do not respect a potential default value)
            values[property.fieldKey] = {};
            setDefaultValues(property.conditions[0], values[property.fieldKey] as Record<string, unknown>);
        }
      });
    }
    setDefaultValues(formFields, baseValues as Record<string, unknown>);

    return baseValues;
  }, [formFields, initialValues, isDraft, isEditMode]);

  return {
    initialValues: startValues,
    formFields,
    jsonSchema,
  };
}

// As validation schema depends on what path of oneOf is currently selected in jsonschema
export const useConstructValidationSchema = (jsonSchema: JSONSchema7, formFields: FormBlock): AnySchema =>
  useMemo(() => buildYupFormForJsonSchema(jsonSchema, formFields), [formFields, jsonSchema]);
