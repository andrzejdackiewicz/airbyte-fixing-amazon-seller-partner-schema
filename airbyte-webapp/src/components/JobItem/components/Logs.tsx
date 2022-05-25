import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { LazyLog } from "react-lazylog";
import styled from "styled-components";

const TIMESTAMP_MATCHER =
  /^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9]/;

const LogsView = styled.div<{ isEmpty?: boolean }>`
  padding: 11px ${({ isEmpty }) => (isEmpty ? 42 : 12)}px 20px;
  font-size: 12px;
  line-height: 18px;
  color: ${({ theme }) => theme.darkPrimaryColor};
  font-family: ${({ theme }) => theme.codeFont};
  word-wrap: break-word;
  min-height: ${({ isEmpty }) => (isEmpty ? "auto" : "400px")};

  & .logLine {
    font-size: 10px;
    color: ${({ theme }) => theme.darkPrimaryColor};

    &.highlightLogLine {
      background: ${({ theme }) => theme.greyColor40};
    }

    &:hover {
      background: ${({ theme }) => theme.greyColor30};
    }

    & > a {
      margin-left: 5px;
      margin-right: 10px;
      width: 45px;
    }
  }
`;

type LogsProps = {
  logsArray?: string[];
  logTimestamp?: number;
};

const Logs: React.FC<LogsProps> = ({ logsArray, logTimestamp }) => {
  const logsJoin = logsArray?.length ? logsArray.join("\n") : "No logs available";
  const matchingLineNumbers = useMemo(() => getMatchingLineNumbers(logTimestamp, logsArray), [logsArray, logTimestamp]);

  return (
    <LogsView isEmpty={!logsArray}>
      {logsArray ? (
        <LazyLog
          text={logsJoin}
          lineClassName="logLine"
          highlightLineClassName="highlightLogLine"
          selectableLines
          follow={matchingLineNumbers.length === 0}
          style={{ background: "transparent" }}
          scrollToLine={matchingLineNumbers.length > 0 ? matchingLineNumbers[0] - 1 : undefined}
          highlight={matchingLineNumbers}
        />
      ) : (
        <FormattedMessage id="sources.emptyLogs" />
      )}
    </LogsView>
  );
};

/**
 * Matching the log's line number by time makes the following assumptions:
 * 1. The log's lines are already ordered by time
 * 2. The timestamps used are in the same timezone
 * 3. The error is closer to the end of the log file than the beginning
 * 4. Lines are matched by the start of a line with "YYYY-MM-DD HH:mm:ss" format, like "2022-05-17 23:00:19 DEBUG I am a log message"
 */
const getMatchingLineNumbers = (matchTimestamp: number | undefined, lines: string[] | undefined) => {
  if (!matchTimestamp || !lines || lines.length === 0) {
    return [];
  }

  const matchTimestampSeconds = Math.floor(matchTimestamp / 1000);
  const matchingLineNumbers: number[] = [];

  let lineCounter = lines.length - 1;
  while (lineCounter >= 0) {
    const timeString = lines[lineCounter].match(TIMESTAMP_MATCHER);
    if (timeString) {
      // The resolution of the timestamps in the logs is seconds (no ms), so this will not need to be rounded
      // Log timestamps are always in UTC
      const datetimeSeconds = Date.parse(`${timeString[0].replace(" ", "T")}Z`) / 1000;
      // Due to the fuzziness of timestamp matching, and how the platform may take some time to build the failureReason timestamp from the log line, we are matching line numbers with +/- 1 second from the failure timestamp
      if (datetimeSeconds >= matchTimestampSeconds - 1 && datetimeSeconds <= matchTimestampSeconds + 1) {
        matchingLineNumbers.push(lineCounter + 1);
      } else if (datetimeSeconds < matchTimestampSeconds) {
        // Once we've reached a timestamp earlier than our search, we can stop seeking
        break;
      }
    }
    lineCounter--;
  }

  return [Math.min(...matchingLineNumbers), Math.max(...matchingLineNumbers)];
};

export default Logs;
