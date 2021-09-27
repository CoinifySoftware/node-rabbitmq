export default interface Logger {
  trace(context?: any, message?: string): void;
  debug(context?: any, message?: string): void;
  info(context?: any, message?: string): void;
  warn(context?: any, message?: string): void;
  error(context?: any, message?: string): void;
  fatal(context?: any, message?: string): void;
}
