import { Client } from 'pg'
import {
  PostgreSqlKeyValueStore,
  PostgreSqlIterator
} from './PostgreSqlKeyValueStore'
import { PostgreSqlRangeDb } from './PostgreSqlRangeDb'
import { Bytes } from '@cryptoeconomicslab/primitives'
import { KeyValueStore, RangeRecord } from '@cryptoeconomicslab/db'

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
        const bucket = await kvs.bucket(testBucket)
        await bucket.put(testKey, testValue)
      })
    })
    describe('get', () => {
      beforeEach(async () => {
        const bucket = await kvs.bucket(testBucket)
        await bucket.put(testKey, testValue)
      })
      it('suceed to get', async () => {
        const bucket = await kvs.bucket(testBucket)
        const value = await bucket.get(testKey)
        expect(value).toEqual(testValue)
      })
      it('get null', async () => {
        const bucket = await kvs.bucket(testBucket)
        const value = await bucket.get(testNotFoundKey)
        expect(value).toBeNull()
      })
    })
  })

  describe('PostgreSqlIterator', () => {
    const testKey1 = Bytes.fromString('test_key1')
    const testKey2 = Bytes.fromString('test_key2')
    const testKey3 = Bytes.fromString('test_key3')
    const testBucket = Bytes.fromString('test_bucket')
    const testBucketNotFound = Bytes.fromString('test_bucket_not_found')
    let bucket: KeyValueStore

    beforeEach(async () => {
      bucket = await kvs.bucket(testBucket)
      await bucket.put(testKey1, testKey1)
      await bucket.put(testKey2, testKey2)
      await bucket.put(testKey3, testKey3)
    })
    describe('next', () => {
      it('return key and value', async () => {
        const iter = await bucket.iter(testKey1)
        const keyValue = await iter.next()
        expect(keyValue).toEqual({ key: testKey1, value: testKey1 })
      })
      it('return null', async () => {
        const bucket = await kvs.bucket(testBucketNotFound)
        const iter = await bucket.iter(testKey1)
        const keyValue = await iter.next()
        expect(keyValue).toBeNull()
      })
      it('return multiple sets of key and value', async () => {
        const iter = new PostgreSqlIterator(
          kvs,
          Bytes.concat(Bytes.fromString('root'), testBucket),
          testKey1,
          true,
          2
        )
        const keyValue1 = await iter.next()
        const keyValue2 = await iter.next()
        const keyValue3 = await iter.next()
        const keyValue4 = await iter.next()
        expect(keyValue1).toEqual({ key: testKey1, value: testKey1 })
        expect(keyValue2).toEqual({ key: testKey2, value: testKey2 })
        expect(keyValue3).toEqual({ key: testKey3, value: testKey3 })
        expect(keyValue4).toBeNull()
      })
    })
  })

  describe('PostgreSqlRangeDb', () => {
    const alice = Bytes.fromString('alice')
    const bob = Bytes.fromString('bob')
    const carol = Bytes.fromString('carol')

    let rangeDb: PostgreSqlRangeDb
    beforeEach(async () => {
      rangeDb = new PostgreSqlRangeDb(kvs)
    })

    it('get ranges', async () => {
      await rangeDb.put(0n, 100n, alice)
      await rangeDb.put(100n, 200n, bob)
      await rangeDb.put(200n, 300n, carol)
      const ranges = await rangeDb.get(0n, 300n)
      expect(ranges.length).toEqual(3)
    })
    it('get mid range', async () => {
      await rangeDb.put(0n, 10n, alice)
      await rangeDb.put(10n, 20n, bob)
      await rangeDb.put(20n, 30n, carol)
      const ranges = await rangeDb.get(10n, 15n)
      expect(ranges.length).toEqual(1)
    })
    it('get small range', async () => {
      await rangeDb.put(120n, 150n, alice)
      await rangeDb.put(0n, 20n, bob)
      await rangeDb.put(500n, 600n, carol)
      const ranges = await rangeDb.get(100n, 200n)
      expect(ranges.length).toEqual(1)
    })
    it('get large range', async () => {
      await rangeDb.put(0n, 500n, alice)
      const ranges = await rangeDb.get(100n, 200n)
      expect(ranges.length).toEqual(1)
    })
    it("don't get edge", async () => {
      await rangeDb.put(80n, 100n, alice)
      const ranges = await rangeDb.get(100n, 200n)
      expect(ranges.length).toEqual(0)
    })
    it('del ranges', async () => {
      await rangeDb.put(0n, 100n, alice)
      await rangeDb.put(100n, 200n, bob)
      await rangeDb.put(200n, 300n, carol)
      await rangeDb.del(0n, 300n)
      const ranges = await rangeDb.get(0n, 300n)
      expect(ranges.length).toEqual(0)
    })
    it('update range', async () => {
      await rangeDb.put(0n, 300n, alice)
      await rangeDb.put(100n, 200n, bob)
      const ranges = await rangeDb.get(0n, 300n)
      expect(ranges).toEqual([
        new RangeRecord(0n, 100n, alice),
        new RangeRecord(100n, 200n, bob),
        new RangeRecord(200n, 300n, alice)
      ])
    })

    describe('get', () => {
      const bigNumberStart = 2n ** 34n
      const bigNumberEnd = 2n ** 34n + 500n
      beforeEach(async () => {
        await rangeDb.put(bigNumberStart, bigNumberEnd, alice)
      })
      it('get a range whose start and end are more than 8 bytes', async () => {
        const ranges = await rangeDb.get(bigNumberStart, bigNumberStart + 1000n)
        expect(ranges).toEqual([
          new RangeRecord(bigNumberStart, bigNumberEnd, alice)
        ])
      })
      it('get no ranges', async () => {
        const ranges = await rangeDb.get(2n ** 32n, 2n ** 32n + 1000n)
        expect(ranges.length).toEqual(0)
      })
      it('get ranges correctly', async () => {
        await rangeDb.put(0x100n, 0x120n, alice)
        await rangeDb.put(0x120n, 0x200n, bob)
        await rangeDb.put(0x1000n, 0x1200n, carol)
        const ranges = await rangeDb.get(0n, 0x2000n)
        expect(ranges).toEqual([
          new RangeRecord(0x100n, 0x120n, alice),
          new RangeRecord(0x120n, 0x200n, bob),
          new RangeRecord(0x1000n, 0x1200n, carol)
        ])
      })
    })

    describe('put', () => {
      const bigNumberIndex1 = 2n ** 34n
      const bigNumberIndex2 = bigNumberIndex1 + 1000n
      const bigNumberIndex3 = bigNumberIndex1 + 2000n
      beforeEach(async () => {
        await rangeDb.put(bigNumberIndex1, bigNumberIndex2, alice)
        await rangeDb.put(bigNumberIndex2, bigNumberIndex3, bob)
      })
      it('put to former', async () => {
        await rangeDb.put(bigNumberIndex1, bigNumberIndex1 + 500n, carol)
        const ranges = await rangeDb.get(bigNumberIndex1, bigNumberIndex3)
        expect(ranges).toEqual([
          new RangeRecord(bigNumberIndex1, bigNumberIndex1 + 500n, carol),
          new RangeRecord(bigNumberIndex1 + 500n, bigNumberIndex2, alice),
          new RangeRecord(bigNumberIndex2, bigNumberIndex3, bob)
        ])
      })
      it('put to middle', async () => {
        await rangeDb.put(bigNumberIndex1 + 200n, bigNumberIndex1 + 500n, carol)
        const ranges = await rangeDb.get(bigNumberIndex1, bigNumberIndex3)
        expect(ranges).toEqual([
          new RangeRecord(bigNumberIndex1, bigNumberIndex1 + 200n, alice),
          new RangeRecord(
            bigNumberIndex1 + 200n,
            bigNumberIndex1 + 500n,
            carol
          ),
          new RangeRecord(bigNumberIndex1 + 500n, bigNumberIndex2, alice),
          new RangeRecord(bigNumberIndex2, bigNumberIndex3, bob)
        ])
      })
      it('put to later', async () => {
        await rangeDb.put(bigNumberIndex1 + 500n, bigNumberIndex2, carol)
        const ranges = await rangeDb.get(bigNumberIndex1, bigNumberIndex3)
        expect(ranges).toEqual([
          new RangeRecord(bigNumberIndex1, bigNumberIndex1 + 500n, alice),
          new RangeRecord(bigNumberIndex1 + 500n, bigNumberIndex2, carol),
          new RangeRecord(bigNumberIndex2, bigNumberIndex3, bob)
        ])
      })
      it('put across', async () => {
        await rangeDb.put(bigNumberIndex1 + 500n, bigNumberIndex2 + 500n, carol)
        const ranges = await rangeDb.get(bigNumberIndex1, bigNumberIndex3)
        expect(ranges).toEqual([
          new RangeRecord(bigNumberIndex1, bigNumberIndex1 + 500n, alice),
          new RangeRecord(
            bigNumberIndex1 + 500n,
            bigNumberIndex2 + 500n,
            carol
          ),
          new RangeRecord(bigNumberIndex2 + 500n, bigNumberIndex3, bob)
        ])
      })
    })
  })
})
