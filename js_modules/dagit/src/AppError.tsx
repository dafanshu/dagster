import * as React from "react";
import { Toaster, Position, Intent } from "@blueprintjs/core";
import { ErrorResponse, onError } from "apollo-link-error";
import { GraphQLError } from "graphql";
import { showCustomAlert } from "./CustomAlertProvider";

interface DagsterGraphQLError extends GraphQLError {
  stack_trace: string[];
}
interface DagsterErrorResponse extends ErrorResponse {
  graphQLErrors?: ReadonlyArray<DagsterGraphQLError>;
}

const ErrorToaster = Toaster.create({ position: Position.TOP_RIGHT });

export const AppErrorLink = () => {
  return onError((response: DagsterErrorResponse) => {
    if (response.graphQLErrors) {
      response.graphQLErrors.map(error => {
        const message = error.path ? (
          <div>
            [GraphQL error] Error resolving field
            <pre>{error.path.join(" \u2192 ")}</pre>
            Message:
            <pre>{error.message}</pre>
            <AppStackTraceLink
              message={error.message}
              stackTrace={error.stack_trace}
            />
          </div>
        ) : (
          `[GraphQL error] ${error.message}`
        );
        if (error.path) {
        }
        ErrorToaster.show({ message, intent: Intent.DANGER });
        console.error("[GraphQL error]", error);
        return null;
      });
    }
    if (response.networkError) {
      ErrorToaster.show({
        message: `[Network error] ${response.networkError.message}`,
        intent: Intent.DANGER
      });
      console.error("[Network error]", response.networkError);
    }
  });
};

const AppStackTraceLink = ({
  message,
  stackTrace
}: {
  message: string;
  stackTrace?: string[];
}) => {
  if (!stackTrace || !stackTrace.length) {
    return null;
  }

  const stackTraceContent = (
    <div
      style={{
        backgroundColor: "rgba(206, 17, 38, 0.05)",
        border: "1px solid #d17257",
        borderRadius: 3,
        maxWidth: "90vw",
        maxHeight: "80vh",
        padding: "1em 2em",
        overflow: "auto"
      }}
    >
      <div
        style={{
          color: "rgb(41, 50, 56)",
          fontFamily: "Consolas, Menlo, monospace",
          fontSize: "0.85em",
          whiteSpace: "pre",
          overflowX: "auto",
          paddingBottom: "1em"
        }}
      >
        {stackTrace.map((line, i) => (
          <div key={i}>{line}</div>
        ))}
      </div>
    </div>
  );

  return (
    <span
      style={{ cursor: "pointer", textDecoration: "underline" }}
      onClick={() =>
        showCustomAlert({ title: message, body: stackTraceContent })
      }
    >
      View stack trace
    </span>
  );
};
