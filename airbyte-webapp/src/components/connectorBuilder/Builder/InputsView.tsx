import { faGear, faPlus } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { Form, Formik, useField, useFormikContext } from "formik";
import { JSONSchema7 } from "json-schema";
import { useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { FormikPatch } from "core/form/FormikPatch";

import { BuilderFormInput } from "../types";
import { BuilderField } from "./BuilderField";
import styles from "./InputsView.module.scss";

interface InputInEditing {
  key: string;
  definition: JSONSchema7;
  required: boolean;
  isNew?: boolean;
  showDefaultValueField: boolean;
  type: "string" | "integer" | "number" | "array" | "boolean" | "enum";
}

function sluggify(str: string) {
  return str.toLowerCase().replaceAll(/[^a-zA-Z\d]/g, "_");
}

function newInputInEditing(): InputInEditing {
  return {
    key: "",
    definition: {},
    required: false,
    isNew: true,
    showDefaultValueField: false,
    type: "string",
  };
}

function formInputToInputInEditing({ key, definition, required }: BuilderFormInput): InputInEditing {
  return {
    key,
    definition,
    required,
    isNew: false,
    showDefaultValueField: Boolean(definition.default),
    type: definition.enum ? "enum" : (definition.type as InputInEditing["type"]),
  };
}

function inputInEditingToFormInput({
  type,
  showDefaultValueField,
  isNew,
  ...values
}: InputInEditing): BuilderFormInput {
  return {
    ...values,
    definition: {
      ...values.definition,
      type: type === "enum" ? "string" : type,
      // only respect the enum values if the user explicitly selected enum as type
      enum: type === "enum" && values.definition.enum?.length ? values.definition.enum : undefined,
      default: showDefaultValueField ? values.definition.default : undefined,
    },
  };
}

export const InputsView: React.FC = () => {
  const [inputs, , helpers] = useField<BuilderFormInput[]>("inputs");
  const [inputInEditing, setInputInEditing] = useState<InputInEditing | undefined>(undefined);
  const usedKeys = useMemo(() => inputs.value.map((input) => input.key), [inputs.value]);
  const inputInEditValidation = useMemo(
    () =>
      yup.object().shape({
        // make sure key can only occur once
        key: yup
          .string()
          .required("form.empty.error")
          .notOneOf(inputInEditing?.isNew ? usedKeys : usedKeys.filter((key) => key !== inputInEditing?.key)),
        required: yup.bool(),
        definition: yup.object().shape({
          title: yup.string().required("form.empty.error"),
        }),
      }),
    [inputInEditing?.isNew, inputInEditing?.key, usedKeys]
  );
  return (
    <div className={styles.viewContainer}>
      <h2>
        <FormattedMessage id="connectorBuilder.inputsTitle" />
      </h2>
      <Text centered>
        <FormattedMessage id="connectorBuilder.inputsDescription" />
      </Text>
      <Card withPadding className={styles.inputsCard}>
        <ol className={styles.list}>
          {inputs.value.map((input) => (
            <li className={styles.listItem} key={input.key}>
              <div className={styles.itemLabel}>{input.definition.title || input.key}</div>
              <Button
                className={styles.itemButton}
                size="sm"
                variant="secondary"
                aria-label="Edit"
                onClick={() => {
                  setInputInEditing(formInputToInputInEditing(input));
                }}
              >
                <FontAwesomeIcon className={styles.icon} icon={faGear} />
              </Button>
            </li>
          ))}
        </ol>
      </Card>
      <Button
        onClick={() => {
          setInputInEditing(newInputInEditing());
        }}
        icon={<FontAwesomeIcon icon={faPlus} />}
        iconPosition="left"
        variant="secondary"
      >
        <FormattedMessage id="connectorBuilder.addInputButton" />
      </Button>
      {inputInEditing && (
        <Formik
          initialValues={inputInEditing}
          validationSchema={inputInEditValidation}
          onSubmit={(values: InputInEditing) => {
            const newInput = inputInEditingToFormInput(values);
            helpers.setValue(
              inputInEditing.isNew
                ? [...inputs.value, newInput]
                : inputs.value.map((input) => (input.key === inputInEditing.key ? newInput : input))
            );
            setInputInEditing(undefined);
          }}
        >
          <>
            <FormikPatch />
            <InputModal
              inputInEditing={inputInEditing}
              onDelete={() => {
                helpers.setValue(inputs.value.filter((input) => input.key !== inputInEditing.key));
                setInputInEditing(undefined);
              }}
              onClose={() => {
                setInputInEditing(undefined);
              }}
            />
          </>
        </Formik>
      )}
    </div>
  );
};
const InputModal = ({
  inputInEditing,
  onClose,
  onDelete,
}: {
  inputInEditing: InputInEditing;
  onDelete: () => void;
  onClose: () => void;
}) => {
  const { isValid, values, setFieldValue } = useFormikContext<InputInEditing>();
  const { formatMessage } = useIntl();
  const [title, titleMeta] = useField<string | undefined>("definition.title");
  useEffect(() => {
    if (titleMeta.touched) {
      setFieldValue("key", sluggify(title.value || ""));
    }
  }, [setFieldValue, title.value, titleMeta.touched]);

  return (
    <Modal
      size="sm"
      title={
        <FormattedMessage
          id={inputInEditing.isNew ? "connectorBuilder.inputModal.newTitle" : "connectorBuilder.inputModal.editTitle"}
        />
      }
      onClose={onClose}
    >
      <Form>
        <ModalBody className={styles.inputForm}>
          <BuilderField
            path="definition.title"
            type="string"
            label={formatMessage({ id: "connectorBuilder.inputModal.inputName" })}
            tooltip={formatMessage({ id: "connectorBuilder.inputModal.inputNameTooltip" })}
          />
          <BuilderField
            path="key"
            type="string"
            readOnly
            label={formatMessage({ id: "connectorBuilder.inputModal.fieldId" })}
            tooltip={formatMessage(
              { id: "connectorBuilder.inputModal.fieldIdTooltip" },
              {
                syntaxExample: "{{my_input}}",
              }
            )}
          />
          <BuilderField
            path="definition.description"
            optional
            type="string"
            label={formatMessage({ id: "connectorBuilder.inputModal.description" })}
            tooltip={formatMessage({ id: "connectorBuilder.inputModal.descriptionTooltip" })}
          />
          <BuilderField
            path="type"
            type="enum"
            options={["string", "number", "integer", "array", "boolean", "enum"]}
            label={formatMessage({ id: "connectorBuilder.inputModal.type" })}
            tooltip={formatMessage({ id: "connectorBuilder.inputModal.typeTooltip" })}
          />
          {values.type === "enum" && (
            <BuilderField
              path="definition.enum"
              type="array"
              optional
              label={formatMessage({ id: "connectorBuilder.inputModal.enum" })}
              tooltip={formatMessage({ id: "connectorBuilder.inputModal.enumTooltip" })}
            />
          )}
          <BuilderField
            path="definition.airbyte_secret"
            type="boolean"
            optional
            label={formatMessage({ id: "connectorBuilder.inputModal.secret" })}
            tooltip={formatMessage({ id: "connectorBuilder.inputModal.secretTooltip" })}
          />
          <BuilderField
            path="required"
            type="boolean"
            optional
            label={formatMessage({ id: "connectorBuilder.inputModal.required" })}
            tooltip={formatMessage({ id: "connectorBuilder.inputModal.requiredTooltip" })}
          />
          <BuilderField
            path="showDefaultValueField"
            type="boolean"
            optional
            label={formatMessage({ id: "connectorBuilder.inputModal.showDefaultValueField" })}
            tooltip={formatMessage({ id: "connectorBuilder.inputModal.showDefaultValueFieldTooltip" })}
          />
          {values.showDefaultValueField && (
            <BuilderField
              path="definition.default"
              type={values.type}
              options={(values.definition.enum || []) as string[]}
              optional
              label={formatMessage({ id: "connectorBuilder.inputModal.default" })}
            />
          )}
          <BuilderField
            path="definition.placeholder"
            type="string"
            optional
            label={formatMessage({ id: "connectorBuilder.inputModal.placeholder" })}
            tooltip={formatMessage({ id: "connectorBuilder.inputModal.placeholderTooltip" })}
          />
        </ModalBody>
        <ModalFooter>
          {!inputInEditing.isNew && (
            <div className={styles.deleteButtonContainer}>
              <Button variant="danger" type="button" onClick={onDelete}>
                <FormattedMessage id="form.delete" />
              </Button>
            </div>
          )}
          <Button variant="secondary" type="reset" onClick={onClose}>
            <FormattedMessage id="form.cancel" />
          </Button>
          <Button type="submit" disabled={!isValid}>
            <FormattedMessage id={inputInEditing.isNew ? "form.create" : "form.saveChanges"} />
          </Button>
        </ModalFooter>
      </Form>
    </Modal>
  );
};
