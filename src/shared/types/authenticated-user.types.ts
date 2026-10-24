import type { ObjectId } from 'mongodb';

export type TUserIdentity = string | number | ObjectId;

export type TUserRoleReference =
  | string
  | number
  | {
      id?: TUserIdentity | null;
      _id?: TUserIdentity | null;
      [key: string]: unknown;
    };

export type TAuthenticatedUser = {
  id?: TUserIdentity | null;
  _id?: TUserIdentity | null;
  email?: string | null;
  isAnonymous?: boolean;
  isRootAdmin?: boolean;
  roles?: TUserRoleReference[] | null;
  loginProvider?: string | null;
  tokenType?: string | null;
  apiTokenId?: string | null;
  [key: string]: unknown;
};
