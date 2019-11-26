import { Client } from 'pg'
import * as wakkanay from 'wakkanay'
import { Bytes } from 'wakkanay/dist/types/Codables'
import { BatchOperation, Iterator } from 'wakkanay/dist/db/KeyValueStore'

export class ByteUtils {
  public static bytesToBuffer(value: Bytes): Buffer {
    return Buffer.from(value.data)
  }
  public static bufferToBytes(value: Buffer): Bytes {
    return Bytes.from(Uint8Array.from(value))
  }
}

export class PostgreSqlIterator implements Iterator {
  bucketName: Bytes
  bound: Bytes
  constructor(bucketName: Bytes, bound: Bytes) {
    this.bucketName = bucketName
    this.bound = bound
  }
  public next(): Promise<{ key: Bytes; value: Bytes } | null> {
    return new Promise((resolve, reject) => {})
  }
}

export class PostgreSqlKeyValueStore implements wakkanay.db.KeyValueStore {
  client: Client
  rootBucket: PostgreSqlBucket
  static defaultBucketName = Bytes.fromString('root')
  constructor(client: Client) {
    this.client = client
    this.rootBucket = new PostgreSqlBucket(
      this,
      PostgreSqlKeyValueStore.defaultBucketName
    )
  }
  async open(): Promise<void> {
    await this.client.connect()
    await this.initilize()
  }
  async close(): Promise<void> {
    await this.client.end()
  }
  async initilize(): Promise<void> {
    await this.client.query(
      'CREATE TABLE IF NOT EXISTS kvs (bucket bytea NOT NULL, key bytea NOT NULL, value bytea NOT NULL, PRIMARY KEY (bucket, key));'
    )
    await this.client.query(
      'CREATE TABLE IF NOT EXISTS range (bucket bytea NOT NULL, range_start BIGINT NOT NULL, range_end BIGINT NOT NULL, value bytea NOT NULL, PRIMARY KEY (bucket, range_end));'
    )
  }
  async get(key: Bytes): Promise<Bytes | null> {
    return this.rootBucket.get(key)
  }
  async put(key: Bytes, value: Bytes): Promise<void> {
    return this.rootBucket.put(key, value)
  }
  async del(key: Bytes): Promise<void> {
    return this.rootBucket.del(key)
  }
  async batch(operations: BatchOperation[]): Promise<void> {
    return this.rootBucket.batch(operations)
  }
  async iter(bound: Bytes): Promise<Iterator> {
    return this.rootBucket.iter(bound)
  }
  bucket(key: Bytes): wakkanay.db.KeyValueStore {
    return this.rootBucket.bucket(key)
  }
}

export class PostgreSqlBucket implements wakkanay.db.KeyValueStore {
  db: PostgreSqlKeyValueStore
  bucketName: Bytes
  constructor(db: PostgreSqlKeyValueStore, bucketName: Bytes) {
    this.db = db
    this.bucketName = bucketName
  }
  async get(key: Bytes): Promise<Bytes | null> {
    const res = await this.db.client.query(
      'SELECT * FROM kvs WHERE bucket = $1 AND key = $2',
      [ByteUtils.bytesToBuffer(this.bucketName), ByteUtils.bytesToBuffer(key)]
    )
    if (res.rows.length == 0) {
      return null
    } else {
      return ByteUtils.bufferToBytes(res.rows[0].value)
    }
  }
  async put(key: Bytes, value: Bytes): Promise<void> {
    await this.db.client.query(
      'INSERT INTO kvs(bucket, key, value) VALUES($1, $2, $3)',
      [
        ByteUtils.bytesToBuffer(this.bucketName),
        ByteUtils.bytesToBuffer(key),
        ByteUtils.bytesToBuffer(value)
      ]
    )
  }
  async del(key: Bytes): Promise<void> {
    await this.db.client.query(
      'DELETE FROM kvs WHERE bucket = $1 AND key = $1',
      [ByteUtils.bytesToBuffer(this.bucketName), ByteUtils.bytesToBuffer(key)]
    )
  }
  async batch(operations: BatchOperation[]): Promise<void> {
    const makeBatch = async () => {
      await Promise.all(
        operations.map(async op => {
          if (op.type === 'Put') {
            await this.db.client.query(
              'INSERT INTO kvs(bucket, key, value) VALUES($1, $2, $3)',
              [
                ByteUtils.bytesToBuffer(this.bucketName),
                ByteUtils.bytesToBuffer(op.key),
                ByteUtils.bytesToBuffer(op.value)
              ]
            )
          } else if (op.type === 'Del') {
            await this.db.client.query(
              'DELETE * FROM kvs WHERE bucket = $1 AND key = $1',
              [
                ByteUtils.bytesToBuffer(this.bucketName),
                ByteUtils.bytesToBuffer(op.key)
              ]
            )
          }
        })
      )
    }
    try {
      await this.db.client.query('BEGIN')
      await makeBatch()
      await this.db.client.query('COMMIT')
    } catch (e) {
      await this.db.client.query('ROLLBACK')
      throw e
    } finally {
      // What should we do finally?
    }
  }
  async iter(bound: Bytes): Promise<Iterator> {
    return new PostgreSqlIterator(this.bucketName, bound)
  }
  bucket(key: Bytes): wakkanay.db.KeyValueStore {
    return new PostgreSqlBucket(this.db, Bytes.concat(this.bucketName, key))
  }
}
