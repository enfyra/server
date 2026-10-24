export type TMutationErrorCategory =
  | 'unique_violation'
  | 'foreign_key_violation'
  | 'not_null_violation'
  | 'check_violation'
  | 'invalid_value_type'
  | 'validation_failed'
  | 'unknown';

export interface TMutationErrorSummary {
  category: TMutationErrorCategory;
  message: string;
}
