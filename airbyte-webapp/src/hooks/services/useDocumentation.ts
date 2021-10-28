import { useConfig } from "config";
import { UseQueryResult, useQuery } from "react-query";

import { fetchDocumentation } from "core/resources/Documentation";

type UseDocumentationResult = UseQueryResult<string, Error>;

export const documentationKeys = {
  text: (integrationUrl: string) => ["document", integrationUrl] as const,
};

const useDocumentation = (documentationUrl: string): UseDocumentationResult => {
  const { integrationUrl } = useConfig();

  return useQuery(documentationKeys.text(documentationUrl), () =>
    fetchDocumentation(documentationUrl, integrationUrl)
  );
};

export default useDocumentation;
