using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Raven.Database.Storage.LevelDB
{
    public class LevelDBStorage
    {
        private readonly ConcurrentDictionary<string, object> backing = new ConcurrentDictionary<string, object>();
        private readonly ConcurrentDictionary<string, int> keysToTransactionId = new ConcurrentDictionary<string, int>();
        private readonly ConcurrentDictionary<int, LevelDBTransaction> activeTransactions = new ConcurrentDictionary<int, LevelDBTransaction>();

        private int lastTransactionId = 0;

        public LevelDBStorage()
        {

        }

        public void Batch(Action<LevelDBAccessor> actions)
        {
            var transaction = this.CreateTransaction();
            var accessor = new LevelDBAccessor(this, transaction);
            try
            {
                // This won't actually do as we need to support reading written actions pre-write
                actions(accessor);
            }
            catch (Exception ex)
            {
                this.RollbackTransaction(transaction);
                throw;
            }
            this.CommitTransaction(transaction);
        }

        private LevelDBTransaction CreateTransaction()
        {
            Interlocked.Increment(ref this.lastTransactionId);
            var currentTransactions = this.activeTransactions.ToArray();
            var transaction = new LevelDBTransaction(this.lastTransactionId, currentTransactions.Select(x => x.Value).ToArray());
            this.activeTransactions.TryAdd(this.lastTransactionId, transaction);
            return transaction;
        }

        private void RollbackTransaction(LevelDBTransaction transaction)
        {
            transaction.Rollback();
            this.TryPruneObsoleteTransactions();
        }

        private void CommitTransaction(LevelDBTransaction transaction)
        {
            // This would be LevelDB.BatchWrite, I just use a database wide lock, mileage may vary
            lock (this.backing)
            {
                transaction.Commit(this);
            }
            this.TryPruneObsoleteTransactions();
        }

        private void TryPruneObsoleteTransactions()
        {
            var currentTransactions = this.activeTransactions.Values.OrderBy(x => x.Id).ToArray();

            var transactionsWeCanRemove = new List<int>();
            foreach (var transaction in currentTransactions)
            {
                if (transaction.RefCount == 0)
                    transactionsWeCanRemove.Add(transaction.Id);
                else
                    break;
            }

            List<string> keysToClear = new List<string>();

            foreach (var kv in this.keysToTransactionId)
            {
                if (transactionsWeCanRemove.Contains(kv.Value))
                    keysToClear.Add(kv.Key);
            }

            keysToClear.ForEach(key =>
            {
                int ignored;
                this.keysToTransactionId.TryRemove(key, out ignored);
            });

            transactionsWeCanRemove.ForEach(key =>
            {
                LevelDBTransaction ignored;
                this.activeTransactions.TryRemove(key, out ignored);
            });
        }

        public Object Get(string id, LevelDBTransaction transaction)
        {
            // This would ordinarily be using a Snapshot for the transaction in LevelDB
            Object outValue;
            if (this.backing.TryGetValue(id, out outValue))
                return outValue;
            return null;
        }

        public void Put(string id, Object obj, LevelDBTransaction transaction)
        {
            this.keysToTransactionId.AddOrUpdate(id, (key) =>
            {
                transaction.AddOperation(storage => storage.Put(id, obj));
                return transaction.Id;
            },
                                                 (key, oldValue) =>
                                                 {
                                                     // NOTE: This doesn't handle the transaction doing multiple operations on the same key
                                                     throw new Exception("This should be a concurrency exception");
                                                 });
        }

        public void Delete(string id, LevelDBTransaction transaction)
        {
            this.keysToTransactionId.AddOrUpdate(id, (key) =>
            {
                transaction.AddOperation(storage => storage.Delete(id));
                return transaction.Id;
            },
                                                 (key, oldValue) =>
                                                 {
                                                     // NOTE: This doesn't handle the transaction doing multiple operations on the same key
                                                     throw new Exception("This should be a concurrency exception");
                                                 });
        }

        internal void Delete(string id)
        {
            object ignored;
            this.backing.TryRemove(id, out ignored);
        }

        internal void Put(string id, Object obj)
        {
            this.backing[id] = obj;
        }
    }
}