import type {
  getFieldPermissionConditionFields,
  matchFieldPermissionCondition,
  prepareFieldPermissionCondition,
} from '../../src/shared/utils/field-permission-condition.util';

type Assert<T extends true> = T;
type IsAny<T> = 0 extends 1 & T ? true : false;
type Assignable<From, To> = [From] extends [To] ? true : false;

type MatchArguments = Parameters<typeof matchFieldPermissionCondition>;
type PrepareArgument = Parameters<typeof prepareFieldPermissionCondition>[0];
type FieldsArgument = Parameters<typeof getFieldPermissionConditionFields>[0];

type MatchConditionIsUnknown = Assert<
  IsAny<MatchArguments[0]> extends false ? Assignable<unknown, MatchArguments[0]> : false
>;
type MatchRecordIsUnknown = Assert<
  IsAny<MatchArguments[1]> extends false ? Assignable<unknown, MatchArguments[1]> : false
>;
type MatchUserIsUnknown = Assert<
  IsAny<MatchArguments[2]> extends false ? Assignable<unknown, MatchArguments[2]> : false
>;
type PreparationAcceptsUnknown = Assert<
  IsAny<PrepareArgument> extends false ? Assignable<unknown, PrepareArgument> : false
>;
type FieldExtractionAcceptsUnknown = Assert<
  IsAny<FieldsArgument> extends false ? Assignable<unknown, FieldsArgument> : false
>;
type DynamicInputsAreAccepted = Assert<
  Assignable<[unknown, unknown, unknown], MatchArguments>
>;
type UnknownConditionRequiresNarrowing = Assert<
  Assignable<MatchArguments[0], Record<string, unknown>> extends false ? true : false
>;
type UnknownRecordRequiresNarrowing = Assert<
  Assignable<MatchArguments[1], { id: string }> extends false ? true : false
>;
type UnknownUserRequiresNarrowing = Assert<
  Assignable<MatchArguments[2], { roles: string[] }> extends false ? true : false
>;
type MatchResultIsBoolean = Assert<
  IsAny<ReturnType<typeof matchFieldPermissionCondition>> extends false
    ? Assignable<ReturnType<typeof matchFieldPermissionCondition>, boolean>
    : false
>;
type MatchResultCannotLeakARecord = Assert<
  Assignable<ReturnType<typeof matchFieldPermissionCondition>, Record<string, unknown>> extends false
    ? true
    : false
>;
type FieldNamesAreStrings = Assert<
  IsAny<ReturnType<typeof getFieldPermissionConditionFields>[number]> extends false
    ? Assignable<ReturnType<typeof getFieldPermissionConditionFields>, string[]>
    : false
>;
type FieldNamesCannotBecomeNumbers = Assert<
  Assignable<ReturnType<typeof getFieldPermissionConditionFields>, number[]> extends false
    ? true
    : false
>;
