import { Client } from 'pg'
import {
  ByteUtils,
  PostgreSqlKeyValueStore,
  PostgreSqlIterator
} from './PostgreSqlKeyValueStore'
import { PostgreSqlRangeDb } from './PostgreSqlRangeDb'
import { Bytes, BigNumber } from 'wakkanay/dist/types/Codables'
import { RangeRecord, RangeStore } from 'wakkanay/dist/db/RangeStore'
import { KeyValueStore } from 'wakkanay/dist/db'
import { range } from 'wakkanay/dist/ovm'

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

    describe.each([0n, 100n, 2n ** 34n])('PostgreSqlRangeDb: %p', baseStart => {
      function testPut(store: RangeStore, s: bigint, e: bigint, value: Bytes) {
        return store.put(baseStart + s, baseStart + e, value)
      }
      function testGet(store: RangeStore, s: bigint, e: bigint) {
        return store.get(baseStart + s, baseStart + e)
      }
      function testDel(store: RangeStore, s: bigint, e: bigint) {
        return store.del(baseStart + s, baseStart + e)
      }
      function testRangeRecord(s: bigint, e: bigint, value: Bytes) {
        return new RangeRecord(baseStart + s, baseStart + e, value)
      }

      describe.each(['rangedb', 'bucket'])(
        'PostgreSqlRangeDb: %p',
        testDbType => {
          let rangeStore: RangeStore

          beforeEach(async () => {
            rangeStore = new PostgreSqlRangeDb(kvs)
            if (testDbType == 'bucket') {
              rangeStore = await rangeStore.bucket(
                Bytes.fromString('test_bucket')
              )
            }
          })

          it('get ranges', async () => {
            await testPut(rangeStore, 0n, 100n, alice)
            await testPut(rangeStore, 100n, 200n, bob)
            await testPut(rangeStore, 200n, 300n, carol)
            const ranges = await testGet(rangeStore, 0n, 300n)
            expect(ranges).toEqual([
              testRangeRecord(0n, 100n, alice),
              testRangeRecord(100n, 200n, bob),
              testRangeRecord(200n, 300n, carol)
            ])
          })
          it('get mid range', async () => {
            await testPut(rangeStore, 0n, 100n, alice)
            await testPut(rangeStore, 100n, 200n, bob)
            await testPut(rangeStore, 200n, 300n, carol)
            const ranges = await testGet(rangeStore, 100n, 150n)
            expect(ranges).toEqual([testRangeRecord(100n, 200n, bob)])
          })
          it('get small range', async () => {
            await testPut(rangeStore, 120n, 150n, alice)
            await testPut(rangeStore, 0n, 20n, bob)
            await testPut(rangeStore, 500n, 600n, carol)
            const ranges = await testGet(rangeStore, 100n, 200n)
            expect(ranges).toEqual([testRangeRecord(120n, 150n, alice)])
          })
          it('get large range', async () => {
            await testPut(rangeStore, 0n, 500n, alice)
            const ranges = await testGet(rangeStore, 100n, 200n)
            expect(ranges).toEqual([testRangeRecord(0n, 500n, alice)])
          })
          it("don't get edge", async () => {
            await testPut(rangeStore, 80n, 100n, alice)
            const ranges = await testGet(rangeStore, 100n, 200n)
            expect(ranges).toEqual([])
          })
          it('del ranges', async () => {
            await testPut(rangeStore, 0n, 100n, alice)
            await testPut(rangeStore, 100n, 200n, bob)
            await testPut(rangeStore, 200n, 300n, carol)
            await testDel(rangeStore, 0n, 300n)
            const ranges = await testGet(rangeStore, 0n, 300n)
            expect(ranges).toEqual([])
          })
          it('update range', async () => {
            await testPut(rangeStore, 0n, 300n, alice)
            await testPut(rangeStore, 100n, 200n, bob)
            const ranges = await testGet(rangeStore, 0n, 300n)
            expect(ranges).toEqual([
              testRangeRecord(0n, 100n, alice),
              testRangeRecord(100n, 200n, bob),
              testRangeRecord(200n, 300n, alice)
            ])
          })

          describe('get', () => {
            it('get a range', async () => {
              await testPut(rangeStore, 0n, 500n, alice)
              const ranges = await testGet(rangeStore, 0n, 1000n)
              expect(ranges).toEqual([testRangeRecord(0n, 500n, alice)])
            })
            it('get no ranges', async () => {
              await testPut(rangeStore, 0n, 500n, alice)
              const ranges = await testGet(rangeStore, 1000n, 2000n)
              expect(ranges).toEqual([])
            })
            it('get no ranges edge case, later query', async () => {
              await testPut(rangeStore, 0n, 500n, alice)
              const ranges = await testGet(rangeStore, 500n, 1000n)
              expect(ranges).toEqual([])
            })
            it('get no ranges edge case, fomer query', async () => {
              await testPut(rangeStore, 500n, 1000n, alice)
              const ranges = await testGet(rangeStore, 0n, 500n)
              expect(ranges).toEqual([])
            })
            it('get ranges correctly', async () => {
              await rangeStore.put(0x100n, 0x120n, alice)
              await rangeStore.put(0x120n, 0x200n, bob)
              await rangeStore.put(0x1000n, 0x1200n, carol)
              const ranges = await rangeStore.get(0n, 0x2000n)
              expect(ranges).toEqual([
                new RangeRecord(0x100n, 0x120n, alice),
                new RangeRecord(0x120n, 0x200n, bob),
                new RangeRecord(0x1000n, 0x1200n, carol)
              ])
            })
          })

          describe('put', () => {
            beforeEach(async () => {
              await testPut(rangeStore, 0n, 1000n, alice)
              await testPut(rangeStore, 1000n, 2000n, bob)
            })
            it('put to former', async () => {
              await testPut(rangeStore, 0n, 500n, carol)
              const ranges = await testGet(rangeStore, 0n, 2000n)
              expect(ranges).toEqual([
                testRangeRecord(0n, 500n, carol),
                testRangeRecord(500n, 1000n, alice),
                testRangeRecord(1000n, 2000n, bob)
              ])
            })
            it('put to middle', async () => {
              await testPut(rangeStore, 200n, 500n, carol)
              const ranges = await testGet(rangeStore, 0n, 2000n)
              expect(ranges).toEqual([
                testRangeRecord(0n, 200n, alice),
                testRangeRecord(200n, 500n, carol),
                testRangeRecord(500n, 1000n, alice),
                testRangeRecord(1000n, 2000n, bob)
              ])
            })
            it('put to later', async () => {
              await testPut(rangeStore, 500n, 1000n, carol)
              const ranges = await testGet(rangeStore, 0n, 2000n)
              expect(ranges).toEqual([
                testRangeRecord(0n, 500n, alice),
                testRangeRecord(500n, 1000n, carol),
                testRangeRecord(1000n, 2000n, bob)
              ])
            })
            it('put across', async () => {
              await testPut(rangeStore, 500n, 1500n, carol)
              const ranges = await testGet(rangeStore, 0n, 2000n)
              expect(ranges).toEqual([
                testRangeRecord(0n, 500n, alice),
                testRangeRecord(500n, 1500n, carol),
                testRangeRecord(1500n, 2000n, bob)
              ])
            })
          })

          describe('bucket', () => {
            let bucketA: RangeStore
            let bucketB: RangeStore
            beforeEach(async () => {
              bucketA = await rangeStore.bucket(Bytes.fromString('a'))
              bucketB = await rangeStore.bucket(Bytes.fromString('b'))
            })
            it('buckets are independent', async () => {
              await testPut(rangeStore, 0n, 100n, alice)
              await testPut(bucketA, 100n, 200n, bob)
              await testPut(bucketB, 200n, 300n, carol)
              const ranges = await testGet(rangeStore, 0n, 300n)
              const rangesA = await testGet(bucketA, 0n, 300n)
              const rangesB = await testGet(bucketB, 0n, 300n)
              expect(ranges).toEqual([testRangeRecord(0n, 100n, alice)])
              expect(rangesA).toEqual([testRangeRecord(100n, 200n, bob)])
              expect(rangesB).toEqual([testRangeRecord(200n, 300n, carol)])
            })
          })
        }
      )
    })
  })
})
