import { Client } from 'pg'
import * as wakkanay from 'wakkanay'
import { Bytes } from 'wakkanay/dist/types/Codables'
import { RangeRecord, RangeStore } from 'wakkanay/dist/db/RangeStore'
import { ByteUtils, PostgreSqlKeyValueStore } from './PostgreSqlKeyValueStore'

export class PostgreSqlRangeDb implements RangeStore {
  kvs: PostgreSqlKeyValueStore
  client: Client
  bucketName: Bytes
  static defaultBucketName = Bytes.fromString('root')
  constructor(kvs: PostgreSqlKeyValueStore, bucketName?: Bytes) {
    this.kvs = kvs
    this.client = kvs.client
    this.bucketName = bucketName
      ? bucketName
      : PostgreSqlRangeDb.defaultBucketName
  }
  async get(start: number, end: number): Promise<RangeRecord[]> {
    const res = await this.client.query(
      'SELECT * FROM range WHERE bucket = $1 AND range_start <= $3 AND range_end > $2 ORDER BY range_end',
      [this.bucketName, start, end]
    )
    return res.rows.map(
      r =>
        new RangeRecord(
          Number(r.range_start),
          Number(r.range_end),
          ByteUtils.bufferToBytes(r.value)
        )
    )
  }
  async put(start: number, end: number, value: Bytes): Promise<void> {
    try {
      await this.client.query('BEGIN')
      const existingRanges = await this.delBatch(start, end)
      if (existingRanges.length > 0 && existingRanges[0].start < start) {
        await this.putOneRange(
          existingRanges[0].start,
          start,
          existingRanges[0].value
        )
      }
      if (existingRanges.length > 0) {
        const lastRange = existingRanges[existingRanges.length - 1]
        if (end < lastRange.end) {
          await this.putOneRange(end, lastRange.end, lastRange.value)
        }
      }
      await this.putOneRange(start, end, value)
      await this.client.query('COMMIT')
    } catch (e) {
      await this.client.query('ROLLBACK')
      throw e
    } finally {
      // What should we do finally?
    }
  }
  async del(start: number, end: number): Promise<void> {
    await this.client.query(
      'DELETE FROM range WHERE bucket = $1 AND range_start <= $3 AND range_end > $2 RETURNING *',
      [this.bucketName, start, end]
    )
  }
  bucket(key: Bytes): wakkanay.db.RangeStore {
    return new PostgreSqlRangeDb(this.kvs, Bytes.concat(this.bucketName, key))
  }
  private async putOneRange(
    start: number,
    end: number,
    value: Bytes
  ): Promise<void> {
    await this.client.query(
      'INSERT INTO range(bucket, range_start, range_end, value) VALUES($1, $2, $3, $4) ' +
        'ON CONFLICT ON CONSTRAINT range_pkey ' +
        'DO UPDATE SET range_start=$2, value=$4',
      [this.bucketName, start, end, ByteUtils.bytesToBuffer(value)]
    )
  }
  async delBatch(start: number, end: number): Promise<RangeRecord[]> {
    const res = await this.client.query(
      'DELETE FROM range WHERE bucket = $1 AND range_start <= $3 AND range_end > $2 RETURNING *',
      [this.bucketName, start, end]
    )
    return res.rows.map(
      r =>
        new RangeRecord(
          Number(r.range_start),
          Number(r.range_end),
          ByteUtils.bufferToBytes(r.value)
        )
    )
  }
}
