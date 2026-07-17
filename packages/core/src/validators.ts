/**
 * GA-PubSub — Validator Adapters
 *
 * Bridges popular schema libraries to the GA-PubSub Validator interface.
 *
 * SUPPORTED:
 *   - Zod (any version that exposes safeParse)
 *   - JSON Schema 7 (via ajv)
 *   - Custom validators (plain functions)
 *
 * USAGE:
 *   import { zodValidator, jsonSchemaValidator, customValidator } from 'ga-pubsub/validators';
 *
 *   // Zod
 *   import { z } from 'zod';
 *   const UserCreated = z.object({ id: z.string(), email: z.string().email() });
 *   bus.registerSchema('user.created', zodValidator('UserCreated', UserCreated));
 *
 *   // JSON Schema
 *   bus.registerSchema('order.placed', jsonSchemaValidator('OrderPlaced', {
 *     type: 'object',
 *     required: ['orderId', 'amount'],
 *     properties: {
 *       orderId: { type: 'string' },
 *       amount:  { type: 'number', minimum: 0 },
 *     }
 *   }));
 *
 *   // Custom
 *   bus.registerSchema('internal.tick', customValidator('Tick', (payload) => {
 *     if (typeof payload !== 'object' || payload === null) {
 *       return { valid: false, errors: [{ path: '', message: 'Must be an object' }] };
 *     }
 *     return { valid: true };
 *   }));
 */

import type { Validator, ValidationResult, ValidationError } from './types.js';

// ─────────────────────────────────────────────────────────────────────────────
// ZOD ADAPTER
// ─────────────────────────────────────────────────────────────────────────────

/** Minimal interface for any Zod-compatible schema */
interface ZodLike {
  safeParse(data: unknown): {
    success: boolean;
    error?: {
      issues: Array<{
        path: (string | number)[];
        message: string;
        code: string;
      }>;
    };
  };
}

/**
 * Adapts a Zod schema to the GA-PubSub Validator interface.
 *
 * @param name  Human-readable schema name (used in error messages)
 * @param schema  Any Zod schema (z.object, z.string, z.union, etc.)
 */
export function zodValidator<T = unknown>(name: string, schema: ZodLike): Validator<T> {
  return {
    name,
    validate(payload: unknown): ValidationResult {
      const result = schema.safeParse(payload);
      if (result.success) return { valid: true };

      const errors: ValidationError[] = (result.error?.issues ?? []).map(issue => ({
        path: issue.path.join('.'),
        message: `[${issue.code}] ${issue.message}`,
        value: undefined,
      }));

      return { valid: false, errors };
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON SCHEMA ADAPTER (AJV)
// ─────────────────────────────────────────────────────────────────────────────

/** Minimal interface for Ajv or compatible validator instances */
interface AjvLike {
  compile(schema: unknown): ((data: unknown) => boolean) & {
    errors?: Array<{
      instancePath: string;
      message?: string;
      data?: unknown;
    }> | null;
  };
}

/**
 * Adapts a JSON Schema + Ajv instance to the GA-PubSub Validator interface.
 *
 * @param name      Human-readable schema name
 * @param schema    JSON Schema object (Draft 7 or later)
 * @param ajv       Ajv instance (v8 recommended: `new Ajv({ allErrors: true })`)
 */
export function jsonSchemaValidator<T = unknown>(
  name: string,
  schema: Record<string, unknown>,
  ajv: AjvLike
): Validator<T> {
  const validate = ajv.compile(schema);

  return {
    name,
    validate(payload: unknown): ValidationResult {
      const valid = validate(payload);
      if (valid) return { valid: true };

      // AJV v8: errors live on the compiled validate function, not the Ajv instance
      const errors: ValidationError[] = (validate.errors ?? []).map(err => ({
        path: err.instancePath || '/',
        message: err.message ?? 'Validation error',
        value: err.data,
      }));

      return { valid: false, errors };
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// CUSTOM VALIDATOR
// ─────────────────────────────────────────────────────────────────────────────

type ValidateFn<T> = (payload: unknown) => ValidationResult | boolean;

/**
 * Wraps a plain validation function as a GA-PubSub Validator.
 *
 * The function may return:
 *   - `true`  → valid
 *   - `false` → invalid with a generic error
 *   - `ValidationResult` → full control over errors
 */
export function customValidator<T = unknown>(
  name: string,
  fn: ValidateFn<T>
): Validator<T> {
  return {
    name,
    validate(payload: unknown): ValidationResult {
      const result = fn(payload);
      if (result === true) return { valid: true };
      if (result === false) {
        return {
          valid: false,
          errors: [{ path: '', message: `Validation failed for schema "${name}"` }],
        };
      }
      return result;
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// VALIBOT ADAPTER
// ─────────────────────────────────────────────────────────────────────────────

/** Minimal interface for Valibot schemas */
interface ValibotSchemaLike {
  _parse(input: unknown): {
    typed: boolean;
    output?: unknown;
    issues?: Array<{ path?: Array<{ key: string | number }>; message: string }>;
  };
}

/**
 * Adapts a Valibot schema to the GA-PubSub Validator interface.
 */
export function valibotValidator<T = unknown>(
  name: string,
  schema: ValibotSchemaLike
): Validator<T> {
  return {
    name,
    validate(payload: unknown): ValidationResult {
      const result = schema._parse(payload);
      if (result.typed && !result.issues?.length) return { valid: true };

      const errors: ValidationError[] = (result.issues ?? []).map(issue => ({
        path: issue.path?.map(p => p.key).join('.') ?? '',
        message: issue.message,
      }));

      return { valid: false, errors: errors.length ? errors : [{ path: '', message: 'Validation failed' }] };
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// COMPOSITE: AND / OR VALIDATORS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Combines validators with AND logic — ALL must pass.
 * Useful for layering a base schema validator with additional business rules.
 */
export function andValidator<T = unknown>(
  name: string,
  ...validators: Validator<T>[]
): Validator<T> {
  return {
    name,
    validate(payload: unknown): ValidationResult {
      const allErrors: ValidationError[] = [];
      for (const v of validators) {
        const result = v.validate(payload);
        if (!result.valid) allErrors.push(...result.errors);
      }
      return allErrors.length
        ? { valid: false, errors: allErrors }
        : { valid: true };
    },
  };
}

/**
 * Combines validators with OR logic — at least ONE must pass.
 * Useful for discriminated unions.
 */
export function orValidator<T = unknown>(
  name: string,
  ...validators: Validator<T>[]
): Validator<T> {
  return {
    name,
    validate(payload: unknown): ValidationResult {
      for (const v of validators) {
        const result = v.validate(payload);
        if (result.valid) return { valid: true };
      }
      return {
        valid: false,
        errors: [{ path: '', message: `None of the OR validators passed for "${name}"` }],
      };
    },
  };
}
