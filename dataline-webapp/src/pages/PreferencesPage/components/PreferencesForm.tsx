import React from "react";
import styled from "styled-components";
import { FormattedMessage, useIntl } from "react-intl";
import { Field, Form, Formik } from "formik";
import * as yup from "yup";

import { BigButton } from "../../../components/CenteredPageComponents";
import LabeledInput from "../../../components/LabeledInput";
import Label from "../../../components/Label";
import LabeledToggle from "../../../components/LabeledToggle";

export type IProps = {
  onSubmit: () => void;
};

const ButtonContainer = styled.div`
  text-align: center;
  margin-top: 38px;
`;

const MainForm = styled(Form)`
  margin-top: 47px;
`;

const FormItem = styled.div`
  margin-bottom: 28px;
`;

const DocsLink = styled.a`
  text-decoration: none;
  color: ${({ theme }) => theme.primaryColor};
  cursor: pointer;
`;

const Subtitle = styled(Label)`
  padding-bottom: 9px;
`;

const Text = styled.div`
  font-style: normal;
  font-weight: normal;
  font-size: 13px;
  line-height: 150%;
  padding-bottom: 9px;
`;

const preferencesValidationSchema = yup.object().shape({
  email: yup.string().email("form.email.error")
});

const PreferencesForm: React.FC<IProps> = ({ onSubmit }) => {
  const formatMessage = useIntl().formatMessage;

  return (
    <Formik
      initialValues={{
        email: "",
        anonymizeData: false,
        news: false,
        security: true
      }}
      validateOnBlur={true}
      validateOnChange={false}
      validationSchema={preferencesValidationSchema}
      onSubmit={async (_, { setSubmitting }) => {
        setSubmitting(false);
        onSubmit();
      }}
    >
      {({ isSubmitting, values }) => (
        <MainForm>
          <FormItem>
            <Field name="email">
              {({ field, meta }) => (
                <LabeledInput
                  {...field}
                  label={<FormattedMessage id="form.emailOptional" />}
                  placeholder={formatMessage({
                    id: "form.email.placeholder"
                  })}
                  type="text"
                  error={!!meta.error && meta.touched}
                  message={
                    meta.touched &&
                    meta.error &&
                    formatMessage({ id: meta.error })
                  }
                />
              )}
            </Field>
          </FormItem>
          <Subtitle>
            <FormattedMessage id="preferences.anonymizeUsage" />
          </Subtitle>
          <Text>
            <FormattedMessage
              id={"preferences.collectData"}
              values={{
                docs: (...docs) => (
                  <DocsLink target="_blank" href="https://dataline.io">
                    {docs}
                  </DocsLink>
                )
              }}
            />
          </Text>
          <FormItem>
            <Field name="anonymizeData">
              {({ field }) => (
                <LabeledToggle
                  {...field}
                  disabled={!values.email}
                  label={<FormattedMessage id="preferences.anonymizeData" />}
                />
              )}
            </Field>
          </FormItem>
          <Subtitle>
            <FormattedMessage id="preferences.news" />
          </Subtitle>
          <FormItem>
            <Field name="news">
              {({ field }) => (
                <LabeledToggle
                  {...field}
                  disabled={!values.email}
                  label={<FormattedMessage id="preferences.featureUpdates" />}
                  message={<FormattedMessage id="preferences.unsubcribeAnyTime" />}
                />
              )}
            </Field>
          </FormItem>
          <Subtitle>
            <FormattedMessage id="preferences.security" />
          </Subtitle>
          <FormItem>
            <Field name="security">
              {({ field }) => (
                <LabeledToggle
                  {...field}
                  disabled={!values.email}
                  label={<FormattedMessage id="preferences.securityUpdates" />}
                />
              )}
            </Field>
          </FormItem>
          <ButtonContainer>
            <BigButton type="submit" disabled={isSubmitting}>
              <FormattedMessage id={"form.continue"} />
            </BigButton>
          </ButtonContainer>
        </MainForm>
      )}
    </Formik>
  );
};

export default PreferencesForm;
