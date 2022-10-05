import React, { useEffect, useState } from "react";
import { FormattedMessage } from "react-intl";

import { Card } from "components";
import { JobItem } from "components/JobItem/JobItem";

import { Action, Namespace } from "core/analytics";
import { Connector, ConnectorSpecification, ConnectorT } from "core/domain/connector";
import { SynchronousJobRead } from "core/request/AirbyteClient";
import { LogsRequestError } from "core/request/LogsRequestError";
import { useAnalyticsService } from "hooks/services/Analytics";
import { generateMessageFromError } from "utils/errorStatusMessage";
import { ServiceForm, ServiceFormProps, ServiceFormValues } from "views/Connector/ServiceForm";

import { ConnectorServiceTypeControl } from "../ServiceForm/components/Controls/ConnectorServiceTypeControl";
import styles from "./ConnectorCard.module.scss";
import { useTestConnector } from "./useTestConnector";

type ConnectorCardProvidedProps = Omit<
  ServiceFormProps,
  "isKeyConnectionInProgress" | "isSuccess" | "onStopTesting" | "testConnector"
>;

interface ConnectorCardBaseProps extends ConnectorCardProvidedProps {
  title?: React.ReactNode;
  full?: boolean;
  jobInfo?: SynchronousJobRead | null;
  additionalDropdownComponent?: React.ReactNode;
  intermediateComponent?: React.ReactNode;
}

interface ConnectorCardCreateProps extends ConnectorCardBaseProps {
  isEditMode?: false;
}

interface ConnectorCardEditProps extends ConnectorCardBaseProps {
  isEditMode: true;
  connector: ConnectorT;
}

export const ConnectorCard: React.FC<ConnectorCardCreateProps | ConnectorCardEditProps> = ({
  title,
  full,
  jobInfo,
  onSubmit,
  additionalDropdownComponent,
  intermediateComponent,
  ...props
}) => {
  const [saved, setSaved] = useState(false);
  const [errorStatusRequest, setErrorStatusRequest] = useState<Error | null>(null);

  const { testConnector, isTestConnectionInProgress, onStopTesting, error, reset } = useTestConnector(props);

  useEffect(() => {
    // Whenever the selected connector changed, reset the check connection call and other errors
    reset();
    setErrorStatusRequest(null);
  }, [props.selectedConnectorDefinitionSpecification, reset]);

  const analyticsService = useAnalyticsService();

  const onHandleSubmit = async (values: ServiceFormValues) => {
    setErrorStatusRequest(null);

    const connector = props.availableServices.find((item) => Connector.id(item) === values.serviceType);

    const trackAction = (actionType: Action, actionDescription: string) => {
      if (!connector) {
        return;
      }

      const namespace = props.formType === "source" ? Namespace.SOURCE : Namespace.DESTINATION;

      analyticsService.track(namespace, actionType, {
        actionDescription,
        connector: connector?.name,
        connector_definition_id: Connector.id(connector),
      });
    };

    const testConnectorWithTracking = async () => {
      trackAction(Action.TEST, "Test a connector");
      try {
        await testConnector(values);
        trackAction(Action.SUCCESS, "Tested connector - success");
      } catch (e) {
        trackAction(Action.FAILURE, "Tested connector - failure");
        throw e;
      }
    };

    try {
      await testConnectorWithTracking();
      await onSubmit(values);
      setSaved(true);
    } catch (e) {
      setErrorStatusRequest(e);
    }
  };

  const job = jobInfo || LogsRequestError.extractJobInfo(errorStatusRequest);

  const { selectedConnectorDefinitionSpecification, onServiceSelect, availableServices, isEditMode } = props;
  const selectedConnectorDefinitionSpecificationId =
    selectedConnectorDefinitionSpecification && ConnectorSpecification.id(selectedConnectorDefinitionSpecification);

  return (
    <>
      <Card title={title} fullWidth={full} className={styles.serviceTypeSelectCard}>
        <div className={styles.dropdownContainer}>
          <ConnectorServiceTypeControl
            formType={props.formType}
            onChangeServiceType={onServiceSelect}
            availableServices={availableServices}
            isEditMode={isEditMode}
            selectedServiceId={selectedConnectorDefinitionSpecificationId}
          />
          {additionalDropdownComponent}
        </div>
      </Card>
      {intermediateComponent}
      <Card>
        <ServiceForm
          {...props}
          errorMessage={props.errorMessage || (error && generateMessageFromError(error))}
          isTestConnectionInProgress={isTestConnectionInProgress}
          onStopTesting={onStopTesting}
          testConnector={testConnector}
          onSubmit={onHandleSubmit}
          successMessage={
            props.successMessage || (saved && props.isEditMode && <FormattedMessage id="form.changesSaved" />)
          }
        />
        {job && <JobItem job={job} />}
      </Card>
    </>
  );
};
