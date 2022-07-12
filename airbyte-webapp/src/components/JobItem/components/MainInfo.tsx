import { faAngleDown } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";
import React from "react";
import { FormattedDateParts, FormattedMessage, FormattedTimeParts } from "react-intl";

import { StatusIcon } from "components";
import { Cell, Row } from "components/SimpleTableComponents";

import { AttemptRead, JobStatus, SynchronousJobRead } from "core/request/AirbyteClient";
import { JobsWithJobs } from "pages/ConnectionPage/pages/ConnectionItemPage/components/JobsList";

import { getJobStatus } from "../JobItem";
import AttemptDetails from "./AttemptDetails";
import styles from "./MainInfo.module.scss";
import { ResetIcon } from "./ResetIcon";
import { ResetStreamsDetails } from "./ResetStreamDetails";

const getJobConfig = (job: SynchronousJobRead | JobsWithJobs) =>
  (job as SynchronousJobRead).configType ?? (job as JobsWithJobs).job.configType;

const getJobCreatedAt = (job: SynchronousJobRead | JobsWithJobs) =>
  (job as SynchronousJobRead).createdAt ?? (job as JobsWithJobs).job.createdAt;

const partialSuccessCheck = (attempts: AttemptRead[]) => {
  if (attempts.length > 0 && attempts[attempts.length - 1].status === JobStatus.failed) {
    return attempts.some((attempt) => attempt.failureSummary && attempt.failureSummary.partialSuccess);
  }
  return false;
};

interface MainInfoProps {
  job: SynchronousJobRead | JobsWithJobs;
  attempts?: AttemptRead[];
  isOpen?: boolean;
  onExpand: () => void;
  isFailed?: boolean;
}

const MainInfo: React.FC<MainInfoProps> = ({ job, attempts = [], isOpen, onExpand, isFailed }) => {
  const jobStatus = getJobStatus(job);
  const isPartialSuccess = partialSuccessCheck(attempts);

  const streamsToReset = (job as JobsWithJobs).job.resetConfig?.streamsToReset;
  const statusIcon = () => {
    switch (true) {
      case Boolean(streamsToReset):
        return <ResetIcon />;
      case jobStatus === JobStatus.cancelled:
        return <StatusIcon status="error" />;
      case jobStatus === JobStatus.running:
        return <StatusIcon status="loading" />;
      case jobStatus === JobStatus.succeeded:
        return <StatusIcon status="success" />;
      case isPartialSuccess:
        return <StatusIcon status="warning" />;
      case !isPartialSuccess && isFailed:
        return <StatusIcon status="error" />;
      default:
        return null;
    }
  };

  const label = () => {
    if (streamsToReset) {
      const formattedMessageId = streamsToReset.length % 10 === 1 ? "sources.streamReset" : "sources.streamsReset";
      return (
        <>
          {streamsToReset.length} <FormattedMessage id={formattedMessageId} />
        </>
      );
    }
    if (isPartialSuccess) {
      return <FormattedMessage id="sources.partialSuccess" />;
    }
    return <FormattedMessage id={`sources.${getJobStatus(job)}`} />;
  };

  return (
    <Row
      className={classNames(styles.mainView, { [styles.failed]: isFailed, [styles.open]: isOpen })}
      onClick={onExpand}
    >
      <Cell className={styles.titleCell}>
        <div className={styles.statusIcon}>{statusIcon()}</div>
        <div className={styles.justification}>
          {label()}
          {attempts.length > 0 && (
            <>
              {attempts.length > 1 && (
                <div className={styles.lastAttempt}>
                  <FormattedMessage id="sources.lastAttempt" />
                </div>
              )}
              {!streamsToReset && (
                <AttemptDetails attempt={attempts[attempts.length - 1]} configType={getJobConfig(job)} />
              )}
              {streamsToReset && <ResetStreamsDetails names={streamsToReset.map((stream) => stream.name)} />}
            </>
          )}
        </div>
      </Cell>
      <Cell className={styles.timestampCell}>
        <div>
          <FormattedTimeParts value={getJobCreatedAt(job) * 1000} hour="numeric" minute="2-digit">
            {(parts) => <span>{`${parts[0].value}:${parts[2].value}${parts[4].value} `}</span>}
          </FormattedTimeParts>
          <FormattedDateParts value={getJobCreatedAt(job) * 1000} month="2-digit" day="2-digit">
            {(parts) => <span>{`${parts[0].value}/${parts[2].value}`}</span>}
          </FormattedDateParts>
          {attempts.length > 1 && (
            <div className={styles.attemptCount}>
              <FormattedMessage id="sources.countAttempts" values={{ count: attempts.length }} />
            </div>
          )}
        </div>
        <FontAwesomeIcon className={styles.arrow} icon={faAngleDown} />
      </Cell>
    </Row>
  );
};

export default MainInfo;
