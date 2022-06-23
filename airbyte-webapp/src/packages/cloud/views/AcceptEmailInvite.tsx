import { Formik } from "formik";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { H1, LoadingButton } from "components";
import HeadTitle from "components/HeadTitle";

import { FieldError } from "../lib/errors/FieldError";
import { useAuthService } from "../services/auth/AuthService";
import { EmailLinkErrorCodes } from "../services/auth/types";
import { BottomBlock, BottomBlockStatusMessage, FieldItem, Form } from "./auth/components/FormComponents";
import {
  EmailField,
  NameField,
  NewsField,
  PasswordField,
  SecurityField,
} from "./auth/SignupPage/components/SignupForm";

const ValidationSchema = yup.object().shape({
  name: yup.string().required("form.empty.error"),
  email: yup.string().email("form.email.error").required("form.empty.error"),
  password: yup.string().min(12, "signup.password.minLength").required("form.empty.error"),
  security: yup.boolean().oneOf([true], "form.empty.error"),
});

export const AcceptEmailInvite: React.FC = () => {
  const { formatMessage } = useIntl();
  const authService = useAuthService();

  const formElement = (
    <Formik
      initialValues={{
        name: "",
        email: "",
        password: "",
        news: true,
        security: false,
      }}
      validationSchema={ValidationSchema}
      onSubmit={async ({ name, email, password, news }, { setFieldError, setStatus }) => {
        try {
          await authService.signUpWithEmailLink({ name, email, password, news });
        } catch (err) {
          if (err instanceof FieldError) {
            setFieldError(err.field, err.message);
          } else {
            setStatus(
              formatMessage({
                id: [EmailLinkErrorCodes.LINK_EXPIRED, EmailLinkErrorCodes.LINK_INVALID].includes(err.message)
                  ? `login.${err.message}`
                  : "errorView.unknownError",
              })
            );
          }
        }
      }}
    >
      {({ isSubmitting, status, values, isValid }) => (
        <Form>
          <FieldItem>
            <NameField />
          </FieldItem>
          <FieldItem>
            <EmailField label={<FormattedMessage id="login.inviteEmail" />} />
          </FieldItem>
          <FieldItem>
            <PasswordField label={<FormattedMessage id="login.createPassword" />} />
          </FieldItem>
          <FieldItem>
            <NewsField />
            <SecurityField />
          </FieldItem>
          <BottomBlock>
            <LoadingButton
              type="submit"
              isLoading={isSubmitting}
              disabled={!isValid || !values.security}
              data-testid="login.signup"
            >
              <FormattedMessage id="login.signup" />
            </LoadingButton>
            {status && <BottomBlockStatusMessage>{status}</BottomBlockStatusMessage>}
          </BottomBlock>
        </Form>
      )}
    </Formik>
  );

  return (
    <>
      <HeadTitle titles={[{ id: "login.inviteTitle" }]} />
      <H1 bold>
        <FormattedMessage id="login.inviteTitle" />
      </H1>
      {formElement}
    </>
  );
};
