import { dump } from "js-yaml";
import merge from "lodash/merge";
import React, { useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { useIntl } from "react-intl";
import { useLocalStorage } from "react-use";

import { BuilderFormValues, convertToManifest, DEFAULT_BUILDER_FORM_VALUES } from "components/connectorBuilder/types";

import { StreamReadRequestBodyConfig, StreamsListReadStreamsItem } from "core/request/ConnectorBuilderClient";
import { ConnectorManifest, DeclarativeComponentSchema } from "core/request/ConnectorManifest";

import { useListStreams } from "./ConnectorBuilderApiService";

const DEFAULT_JSON_MANIFEST_VALUES: ConnectorManifest = {
  version: "0.1.0",
  type: "DeclarativeSource",
  check: {
    type: "CheckStream",
    stream_names: [],
  },
  streams: [],
};

export type EditorView = "ui" | "yaml";
export type BuilderView = "global" | "inputs" | number;

interface StateContext {
  builderFormValues: BuilderFormValues;
  jsonManifest: ConnectorManifest;
  lastValidJsonManifest: DeclarativeComponentSchema | undefined;
  yamlManifest: string;
  yamlEditorIsMounted: boolean;
  yamlIsValid: boolean;
  testStreamIndex: number;
  selectedView: BuilderView;
  editorView: EditorView;
  setBuilderFormValues: (values: BuilderFormValues, isInvalid: boolean) => void;
  setJsonManifest: (jsonValue: ConnectorManifest) => void;
  setYamlEditorIsMounted: (value: boolean) => void;
  setYamlIsValid: (value: boolean) => void;
  setTestStreamIndex: (streamIndex: number) => void;
  setSelectedView: (view: BuilderView) => void;
  setEditorView: (editorView: EditorView) => void;
}

interface APIContext {
  streams: StreamsListReadStreamsItem[];
  streamListErrorMessage: string | undefined;
  configJson: StreamReadRequestBodyConfig;
  setConfigJson: (value: StreamReadRequestBodyConfig) => void;
}

export const ConnectorBuilderStateContext = React.createContext<StateContext | null>(null);
export const ConnectorBuilderAPIContext = React.createContext<APIContext | null>(null);

export const ConnectorBuilderStateProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  // manifest values
  const [storedBuilderFormValues, setStoredBuilderFormValues] = useLocalStorage<BuilderFormValues>(
    "connectorBuilderFormValues",
    DEFAULT_BUILDER_FORM_VALUES
  );

  const lastValidBuilderFormValuesRef = useRef<BuilderFormValues>(storedBuilderFormValues as BuilderFormValues);

  const setBuilderFormValues = useCallback(
    (values: BuilderFormValues, isValid: boolean) => {
      setStoredBuilderFormValues(values);
      if (isValid) {
        lastValidBuilderFormValuesRef.current = values;
      }
    },
    [setStoredBuilderFormValues]
  );

  const builderFormValues = useMemo(() => {
    return merge({}, DEFAULT_BUILDER_FORM_VALUES, storedBuilderFormValues);
  }, [storedBuilderFormValues]);

  const [jsonManifest, setJsonManifest] = useLocalStorage<ConnectorManifest>(
    "connectorBuilderJsonManifest",
    DEFAULT_JSON_MANIFEST_VALUES
  );
  const manifest = jsonManifest ?? DEFAULT_JSON_MANIFEST_VALUES;

  const [editorView, setEditorView] = useState<EditorView>("ui");

  const derivedJsonManifest = useMemo(
    () => (editorView === "yaml" ? manifest : convertToManifest(builderFormValues)),
    [editorView, builderFormValues, manifest]
  );

  const [yamlIsValid, setYamlIsValid] = useState(true);
  const [yamlEditorIsMounted, setYamlEditorIsMounted] = useState(true);

  const yamlManifest = useMemo(() => dump(derivedJsonManifest), [derivedJsonManifest]);

  const lastValidBuilderFormValues = lastValidBuilderFormValuesRef.current;
  /**
   * The json manifest derived from the last valid state of the builder form values.
   * In the yaml view, this is undefined. Can still be invalid in case an invalid state is loaded from localstorage
   */
  const lastValidJsonManifest = useMemo(
    () =>
      editorView !== "ui"
        ? undefined
        : builderFormValues === lastValidBuilderFormValues
        ? jsonManifest
        : convertToManifest(lastValidBuilderFormValues),
    [builderFormValues, editorView, jsonManifest, lastValidBuilderFormValues]
  );

  const [testStreamIndex, setTestStreamIndex] = useState(0);

  const [selectedView, setSelectedView] = useState<BuilderView>("global");

  const ctx = {
    builderFormValues,
    jsonManifest: derivedJsonManifest,
    lastValidJsonManifest,
    yamlManifest,
    yamlEditorIsMounted,
    yamlIsValid,
    testStreamIndex,
    selectedView,
    editorView,
    setBuilderFormValues,
    setJsonManifest,
    setYamlIsValid,
    setYamlEditorIsMounted,
    setTestStreamIndex,
    setSelectedView,
    setEditorView,
  };

  return <ConnectorBuilderStateContext.Provider value={ctx}>{children}</ConnectorBuilderStateContext.Provider>;
};

export const ConnectorBuilderAPIProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { formatMessage } = useIntl();
  const { lastValidJsonManifest, testStreamIndex, setTestStreamIndex } = useConnectorBuilderState();

  const manifest = lastValidJsonManifest ?? DEFAULT_JSON_MANIFEST_VALUES;

  // config
  const [configJson, setConfigJson] = useState<StreamReadRequestBodyConfig>({});

  // streams
  const {
    data: streamListRead,
    isError: isStreamListError,
    error: streamListError,
  } = useListStreams({ manifest, config: configJson });
  const unknownErrorMessage = formatMessage({ id: "connectorBuilder.unknownError" });
  const streamListErrorMessage = isStreamListError
    ? streamListError instanceof Error
      ? streamListError.message || unknownErrorMessage
      : unknownErrorMessage
    : undefined;
  const streams = useMemo(() => {
    return streamListRead?.streams ?? [];
  }, [streamListRead]);

  useEffect(() => {
    if (testStreamIndex >= streams.length && streams.length > 0) {
      setTestStreamIndex(streams.length - 1);
    }
  }, [streams, testStreamIndex, setTestStreamIndex]);

  const ctx = {
    streams,
    streamListErrorMessage,
    configJson,
    setConfigJson,
  };

  return <ConnectorBuilderAPIContext.Provider value={ctx}>{children}</ConnectorBuilderAPIContext.Provider>;
};

export const useConnectorBuilderAPI = (): APIContext => {
  const connectorBuilderState = useContext(ConnectorBuilderAPIContext);
  if (!connectorBuilderState) {
    throw new Error("useConnectorBuilderAPI must be used within a ConnectorBuilderAPIProvider.");
  }

  return connectorBuilderState;
};

export const useConnectorBuilderState = (): StateContext => {
  const connectorBuilderState = useContext(ConnectorBuilderStateContext);
  if (!connectorBuilderState) {
    throw new Error("useConnectorBuilderState must be used within a ConnectorBuilderStateProvider.");
  }

  return connectorBuilderState;
};

export const useSelectedPageAndSlice = () => {
  const { testStreamIndex } = useConnectorBuilderState();
  const { streams } = useConnectorBuilderAPI();

  const selectedStreamName = streams[testStreamIndex].name;

  const [streamToSelectedSlice, setStreamToSelectedSlice] = useState({ [selectedStreamName]: 0 });
  const setSelectedSlice = (sliceIndex: number) => {
    setStreamToSelectedSlice((prev) => {
      return { ...prev, [selectedStreamName]: sliceIndex };
    });
  };
  const selectedSlice = streamToSelectedSlice[selectedStreamName] ?? 0;

  const [streamToSelectedPage, setStreamToSelectedPage] = useState({ [selectedStreamName]: 0 });
  const setSelectedPage = (pageIndex: number) => {
    setStreamToSelectedPage((prev) => {
      return { ...prev, [selectedStreamName]: pageIndex };
    });
  };
  const selectedPage = streamToSelectedPage[selectedStreamName] ?? 0;

  return { selectedSlice, selectedPage, setSelectedSlice, setSelectedPage };
};
