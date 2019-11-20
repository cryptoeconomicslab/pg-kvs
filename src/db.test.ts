import { Client } from 'pg'
import { ByteUtils, PostgreSqlKeyValueStore } from './PostgreSqlKeyValueStore'
import { PostgreSqlRangeDb } from './PostgreSqlRangeDb'
import { Bytes } from 'wakkanay/dist/types/Codables'

const testDbName = Bytes.fromString('test_pg')
const testBucket = Bytes.fromString('test_bucket')
const testKey = Bytes.fromString('test_key')
const testNotFoundKey = Bytes.fromString('test_not_found_key')
const testValue = Bytes.fromString('test_value')

describe('DB', () => {
  let kvs: PostgreSqlKeyValueStore
  beforeEach(async () => {
    const client = new Client({
      user: 'postgres',
      host: 'localhost',
      database: 'postgres',
      port: 5432
    })
    kvs = new PostgreSqlKeyValueStore(client)
    await kvs.open()
    await client.query('DELETE FROM kvs')
    await client.query('DELETE FROM range')
  })
  afterEach(async () => {
    await kvs.close()
  })
  describe('put', () => {
    it('suceed to put', async () => {
      await kvs.put(testKey, testValue)
    })
  })
  describe('get', () => {
    beforeEach(async () => {
      await kvs.put(testKey, testValue)
    })
    it('suceed to get', async () => {
      const value = await kvs.get(testKey)
      expect(value).toEqual(testValue)
    })
    it('get null', async () => {
      const value = await kvs.get(testNotFoundKey)
      expect(value).toBeNull()
    })
  })
  describe('bucket', () => {
    describe('put', () => {
      it('suceed to put', async () => {
        const bucket = kvs.bucket(testBucket)
        await bucket.put(testKey, testValue)
      })
    })
    describe('get', () => {
      beforeEach(async () => {
        const bucket = kvs.bucket(testBucket)
        await bucket.put(testKey, testValue)
      })
      it('suceed to get', async () => {
        const bucket = kvs.bucket(testBucket)
        const value = await bucket.get(testKey)
        expect(value).toEqual(testValue)
      })
      it('get null', async () => {
        const bucket = kvs.bucket(testBucket)
        const value = await bucket.get(testNotFoundKey)
        expect(value).toBeNull()
      })
    })
  })
  describe('PostgreSqlRangeDb', () => {
    let rangeDb: PostgreSqlRangeDb
    beforeEach(async () => {
      rangeDb = new PostgreSqlRangeDb(kvs)
    })
    describe('put', () => {
      const testUpdate = Bytes.fromString('test_update')
      beforeEach(async () => {
        await rangeDb.put(100, 150, testValue)
      })
      it('suceed to put', async () => {
        await rangeDb.put(10, 20, testUpdate)
      })
      it('suceed to put a range and update existing', async () => {
        await rangeDb.put(50, 150, testUpdate)
        const ranges = await rangeDb.get(50, 150)
        expect(ranges.length).toBe(1)
        expect(ranges[0].start).toBe(50)
        expect(ranges[0].end).toBe(150)
        expect(ranges[0].value).toEqual(testUpdate)
      })
      it('suceed to update a range within existing range', async () => {
        await rangeDb.put(110, 120, testUpdate)
        const ranges = await rangeDb.get(100, 150)
        expect(ranges.length).toBe(3)
        expect(ranges[0].start).toBe(100)
        expect(ranges[0].end).toBe(110)
        expect(ranges[0].value).toEqual(testValue)
        expect(ranges[1].start).toBe(110)
        expect(ranges[1].end).toBe(120)
        expect(ranges[1].value).toEqual(testUpdate)
        expect(ranges[2].start).toBe(120)
        expect(ranges[2].end).toBe(150)
        expect(ranges[2].value).toEqual(testValue)
      })
    })
    describe('get', () => {
      beforeEach(async () => {
        await rangeDb.put(100, 200, testValue)
        await rangeDb.put(200, 300, testValue)
      })
      it('suceed to get', async () => {
        const ranges = await rangeDb.get(100, 110)
        expect(ranges.length).toBe(1)
        expect(ranges[0].start).toBe(100)
        expect(ranges[0].end).toBe(200)
        expect(ranges[0].value).toEqual(testValue)
      })
      it('get nothing', async () => {
        const ranges = await rangeDb.get(500, 600)
        expect(ranges).toEqual([])
      })
      it('get multiple ranges', async () => {
        const ranges = await rangeDb.get(100, 250)
        expect(ranges.length).toBe(2)
      })
    })
    describe('del', () => {
      it('suceed to del', async () => {
        await rangeDb.del(0, 50)
      })
    })
  })
})
