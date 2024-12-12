use std::result;

use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum Errors {
    #[error("failed to read from data file")]
    FailedToReadDataFromDataFile,

    #[error("failed to write to data file")]
    FailedToWriteDataToDataFile,

    #[error("failed to sync data file")]
    FailedSyncDataFile,

    #[error("failed to open data file")]
    FailedOpenDataFile,

    #[error("the key is empty")]
    KeyIsEmpty,

    #[error("the value is empty")]
    ValueIsEmpty,

    #[error("memory index failed to updated")]
    IndexUpdateFailed,

    #[error("key is not found in database")]
    KeyNotFound,

    #[error("data file is not found in database")]
    DataFileNotFound,

    #[error("database dir path can not be empty")]
    DirPathIsEmpty,

    #[error("database data file size must be greater than 0")]
    DataFileSizeTooSmall,

    #[error("failed to create the database dir")]
    FailedToCreateDatabaseDir,

    #[error("failed to read the database dir")]
    FailedToReadDatabaseDir,

    #[error("the database dir maybe corrupted")]
    DataDirCorrupted,

    #[error("read data file eof")]
    ReadDataFileEof,

    #[error("invalid crc value, log record maybe corrupted")]
    InvaildLogRecordCrc,

    #[error("key conflicts with other transactions")]
    MvccTxnWriteKeyConflictsWithOtherTransactions,

    #[error("active txn is not exist")]
    MvccCommitActiveTxnIsNotExist,

    #[error("rollback delete data failed")]
    MvccRollbackDeleteDataFailed,

    #[error("exceed the max batch num")]
    ExceedMaxBatchNum,

    #[error("merge is in progress")]
    MergeInProgress,

    #[error("the database directory is used by another process")]
    DatabaseIsUsing,

    #[error("unable to use wirte batch, seq file not exists")]
    UableToUseWriteBatch,

    #[error("invaild data file merge ratio")]
    InvaildDataFileMergeRatio,

    #[error("merge ratio unreached")]
    MergeRatioUnreached,

    #[error("disk space is not enough for merge")]
    MergeNoEnoughSpace,
}

pub type Result<T> = result::Result<T, Errors>;
