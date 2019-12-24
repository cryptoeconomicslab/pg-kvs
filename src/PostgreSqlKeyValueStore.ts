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
  private buffer: { key: Bytes; value: Bytes }[] = []
  private isFirstFetch = true
  constructor(
    private db: PostgreSqlKeyValueStore,
    private bucketName: Bytes,
    private bound: Bytes,
    private lowerBoundExclusive: boolean,
    private limit: number = 10
  ) {}

  public async next(): Promise<{ key: Bytes; value: Bytes } | null> {
    if (this.buffer.length == 0) {
      this.buffer = await this.fetch(
        this.isFirstFetch && this.lowerBoundExclusive
      )
      this.isFirstFetch = false
      if (this.buffer.length > 0) {
        this.bound = this.buffer[this.buffer.length - 1].key
      }
    }
    const result = this.buffer.shift()
    return result ? result : null
  }

  private async fetch(
    isFirst: boolean
  ): Promise<{ key: Bytes; value: Bytes }[]> {
    const res = await this.db.client.query(
      isFirst
        ? 'SELECT * FROM kvs WHERE bucket = $1 AND key >= $2 ORDER BY key LIMIT $3'
        : 'SELECT * FROM kvs WHERE bucket = $1 AND key > $2 ORDER BY key LIMIT $3',
      [
        ByteUtils.bytesToBuffer(this.bucketName),
        ByteUtils.bytesToBuffer(this.bound),
        this.limit
      ]
    )
    return res.rows.map(r => {
      return {
        key: ByteUtils.bufferToBytes(r.key),
        value: ByteUtils.bufferToBytes(r.value)
      }
    })
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
      'CREATE TABLE IF NOT EXISTS range (bucket bytea NOT NULL, range_start bytea NOT NULL, range_end bytea NOT NULL, value bytea NOT NULL, PRIMARY KEY (bucket, range_end));'
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
  iter(lowerBound: Bytes, lowerBoundExclusive?: boolean | undefined): Iterator {
    return this.rootBucket.iter(lowerBound, lowerBoundExclusive)
  }
  async bucket(key: Bytes): Promise<wakkanay.db.KeyValueStore> {
    return this.rootBucket.bucket(key)
  }
}

export class PostgreSqlBucket implements wakkanay.db.KeyValueStore {
  close(): Promise<void> {
    throw new Error('Method not implemented.')
  }
  open(): Promise<void> {
    throw new Error('Method not implemented.')
  }
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
  iter(lowerBound: Bytes, lowerBoundExclusive?: boolean | undefined): Iterator {
    return new PostgreSqlIterator(
      this.db,
      this.bucketName,
      lowerBound,
      lowerBoundExclusive !== undefined ? lowerBoundExclusive : true
    )
  }
  async bucket(key: Bytes): Promise<wakkanay.db.KeyValueStore> {
    return new PostgreSqlBucket(this.db, Bytes.concat(this.bucketName, key))
  }
}
