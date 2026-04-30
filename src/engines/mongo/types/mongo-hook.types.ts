export type MongoHookEvent =
  | 'beforeInsert'
  | 'afterInsert'
  | 'beforeUpdate'
  | 'afterUpdate'
  | 'beforeDelete'
  | 'afterDelete'
  | 'beforeSelect'
  | 'afterSelect';

export interface MongoHookContext {
  collectionName: string;
  operation:
    | 'insert'
    | 'update'
    | 'delete'
    | 'select'
    | 'findOne'
    | 'count';
  originalData?: any;
  originalFilter?: any;
  recordId?: any;
  oldRecord?: any;
  deletedRecord?: any;
  relationData?: any;
  insertedId?: any;
  updateResult?: any;
  deleteResult?: any;
}

export interface MongoHookRegistry {
  beforeInsert: Array<
    (
      collectionName: string,
      data: any,
      context: MongoHookContext,
    ) => any | Promise<any>
  >;
  afterInsert: Array<
    (
      collectionName: string,
      result: any,
      context: MongoHookContext,
    ) => any | Promise<any>
  >;
  beforeUpdate: Array<
    (
      collectionName: string,
      data: any,
      context: MongoHookContext,
    ) => any | Promise<any>
  >;
  afterUpdate: Array<
    (
      collectionName: string,
      result: any,
      context: MongoHookContext,
    ) => any | Promise<any>
  >;
  beforeDelete: Array<
    (
      collectionName: string,
      filter: any,
      context: MongoHookContext,
    ) => any | Promise<any>
  >;
  afterDelete: Array<
    (
      collectionName: string,
      result: any,
      context: MongoHookContext,
    ) => any | Promise<any>
  >;
  beforeSelect: Array<
    (
      collectionName: string,
      filter: any,
      context: MongoHookContext,
    ) => any | Promise<any>
  >;
  afterSelect: Array<
    (
      collectionName: string,
      result: any,
      context: MongoHookContext,
    ) => any | Promise<any>
  >;
}

export type MongoHookHandler<E extends MongoHookEvent> =
  MongoHookRegistry[E][number];
