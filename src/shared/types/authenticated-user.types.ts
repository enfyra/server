export type TUserRoleReference =
  | string
  | number
  | {
      id?: string | number | null;
      _id?: string | number | null;
    };

export type TAuthenticatedUser = {
  id?: string | number | null;
  _id?: string | number | null;
  isAnonymous?: boolean;
  isRootAdmin?: boolean;
  roles?: TUserRoleReference[] | null;
};
