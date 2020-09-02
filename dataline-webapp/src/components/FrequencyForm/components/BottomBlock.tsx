import React from "react";
import styled from "styled-components";
import { FormattedMessage } from "react-intl";

import Button from "../../Button";
import Spinner from "../../Spinner";
import StatusIcon from "../../StatusIcon";

type IProps = {
  isSubmitting: boolean;
  isValid: boolean;
  dirty: boolean;
  errorMessage?: React.ReactNode;
};

const ButtonContainer = styled.div`
  margin-top: 34px;
  display: flex;
  align-items: center;
  justify-content: space-between;
`;

const LoadingContainer = styled(ButtonContainer)`
  font-weight: 600;
  font-size: 14px;
  line-height: 17px;
  color: ${({ theme }) => theme.darkPrimaryColor};
  justify-content: center;
`;

const Loader = styled.div`
  margin-right: 10px;
`;

const Success = styled(StatusIcon)`
  width: 26px;
  min-width: 26px;
  height: 26px;
  padding-top: 5px;
  font-size: 17px;
`;

const Error = styled(Success)`
  padding-top: 4px;
  padding-left: 1px;
`;

const ErrorBlock = styled.div`
  display: flex;
  justify-content: right;
  align-items: center;
  font-weight: 600;
  font-size: 12px;
  line-height: 18px;
  color: ${({ theme }) => theme.darkPrimaryColor};
`;

const ErrorText = styled.div`
  font-weight: normal;
  color: ${({ theme }) => theme.dangerColor};
  max-width: 400px;
`;

const BottomBlock: React.FC<IProps> = ({
  isSubmitting,
  isValid,
  dirty,
  errorMessage
}) => {
  if (isSubmitting) {
    return (
      <LoadingContainer>
        <Loader>
          <Spinner />
        </Loader>
        <FormattedMessage id="form.testingConnection" />
      </LoadingContainer>
    );
  }

  return (
    <ButtonContainer>
      {errorMessage ? (
        <ErrorBlock>
          <Error />
          <div>
            <FormattedMessage id="form.failedTests" />
            <ErrorText>{errorMessage}</ErrorText>
          </div>
        </ErrorBlock>
      ) : (
        <div />
      )}
      <Button type="submit" disabled={isSubmitting || !isValid || !dirty}>
        <FormattedMessage id="onboarding.setUpConnection" />
      </Button>
    </ButtonContainer>
  );
};

export default BottomBlock;
