import { Client } from 'pg'
import {
  PostgreSqlKeyValueStore,
  PostgreSqlIterator
} from '../src/PostgreSqlKeyValueStore'
import { Bytes } from '@cryptoeconomicslab/primitives'
import { KeyValueStore } from '@cryptoeconomicslab/db'

const testBucket = Bytes.fromString('test_bucket')
const testKey = Bytes.fromString('test_key')
const testNotFoundKey = Bytes.fromString('test_not_found_key')
const testValue = Bytes.fromString('test_value')

describe('PostgreSqlKeyValueStore', () => {
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

  it('succeed to put and get', async () => {
    await kvs.put(testKey, testValue)
    const value = await kvs.get(testKey)
    expect(value).toEqual(testValue)
  })

  it('upsert to already existing key', async () => {
    await kvs.put(testKey, testValue)
    const testValue2 = Bytes.fromString('test_value2')
    await kvs.put(testKey, testValue2)
    const value = await kvs.get(testKey)
    expect(value).toEqual(testValue2)
  })

  it('get null', async () => {
    const value = await kvs.get(testNotFoundKey)
    expect(value).toBeNull()
  })

  describe('bucket', () => {
    it('succeed to put and get', async () => {
      const bucket = await kvs.bucket(testBucket)
      await bucket.put(testKey, testValue)
      const value = await bucket.get(testKey)
      expect(value).toEqual(testValue)
    })

    it('get null', async () => {
      const bucket = await kvs.bucket(testBucket)
      const value = await bucket.get(testNotFoundKey)
      expect(value).toBeNull()
    })

    it('succeed to get values from iterator of bucket', async () => {
      const testDbKey0 = Bytes.fromString('0')
      const testDbKey1 = Bytes.fromString('1')

      const testNotEmptyBucketName = Bytes.fromString('bucket2')
      const testNotEmptyBucket = await kvs.bucket(testNotEmptyBucketName)

      await testNotEmptyBucket.put(testDbKey0, testDbKey0)
      await testNotEmptyBucket.put(testDbKey1, testDbKey1)
      const iter = testNotEmptyBucket.iter(testDbKey0)
      const result0 = await iter.next()
      const result1 = await iter.next()
      expect(result0).not.toBeNull()
      expect(result1).not.toBeNull()
      if (result0 !== null && result1 !== null) {
        expect(result0.key).toEqual(testDbKey0)
        expect(result0.value).toEqual(testDbKey0)
        expect(result1.key).toEqual(testDbKey1)
        expect(result1.value).toEqual(testDbKey1)
      }
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
          false,
          3
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
})
