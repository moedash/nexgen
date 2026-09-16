// Test-only bridge for generated System Nexus bindings. The real SDK supplies
// a workflow-aware wrapper that also preserves the workflow random scope.
type AnyFunction = (...args: any[]) => any;
type Next<I, M extends keyof I> = Required<I>[M] extends AnyFunction
  ? (
      ...args: Parameters<Required<I>[M]> extends [...infer Args, AnyFunction]
        ? Args
        : never
    ) => ReturnType<Required<I>[M]>
  : never;

export function composeInterceptors<I, M extends keyof I>(
  interceptors: I[],
  method: M,
  next: Next<I, M>,
): Next<I, M> {
  let composedNext: AnyFunction = next as AnyFunction;
  for (let index = interceptors.length - 1; index >= 0; index--) {
    const interceptor = interceptors[index];
    if (interceptor?.[method] !== undefined) {
      const previous = composedNext;
      composedNext = (input: unknown) =>
        (interceptor[method] as AnyFunction)(input, previous);
    }
  }
  return composedNext as Next<I, M>;
}
