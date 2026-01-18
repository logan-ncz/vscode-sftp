import { Readable } from 'stream';
import logger from '../../logger';
import { FileEntry, FileType, FileStats, FileOption } from './fileSystem';
import RemoteFileSystem from './remoteFileSystem';
import { FTPClient } from '../remote-client';
import Scheduler from '../scheduler';

interface FtpFileHandle {
  path: string;
  flags: string;
  mode?: number;
}

const numMap = {
  r: 4,
  w: 2,
  x: 1,
};

function toNumMode(rightObj) {
  // some ftp server would reusult rightObj undefined.
  if (!rightObj) return 0o666;

  // tslint:disable-next-line:no-shadowed-variable
  const modeStr = Object.keys(rightObj).reduce((modeStr, key) => {
    const rightStr = rightObj[key];
    let cur = 0;
    for (const char of rightStr) {
      cur += numMap[char];
    }
    return modeStr + cur;
  }, '');

  return parseInt(modeStr, 8);
}

export default class FTPFileSystem extends RemoteFileSystem {
  private _supportMFMT: boolean = true;
  private queue: Scheduler;

  static getFileType(type) {
    if (type === 'd') {
      return FileType.Directory;
    } else if (type === '-') {
      return FileType.File;
    } else if (type === 'l') {
      return FileType.SymbolicLink;
    } else {
      return FileType.Unknown;
    }
  }

  constructor(pathResolver, option) {
    super(pathResolver, option);

    // Initialiser la queue avec une concurrence par défaut de 5
    // Cela permet plusieurs opérations FTP en parallèle tout en évitant
    // l'épuisement des ports sur les serveurs avec une plage limitée (comme VSFTPD)
    // La concurrence peut être ajustée via la configuration ftpConcurrency
    const concurrency = (option && option.ftpConcurrency) || 5;
    this.queue = new Scheduler({ concurrency });
  }

  /**
   * Ferme le socket passif du client FTP de manière sécurisée et immédiate
   * Cette fonction aide à éviter l'épuisement des ports sur les serveurs FTP
   * avec une plage de ports limitée (comme VSFTPD)
   */
  private closePasvSocket(ftpClient: any, delay: number = 0): void {
    setTimeout(() => {
      if (ftpClient && ftpClient._pasvSocket) {
        try {
          if (!ftpClient._pasvSocket.destroyed) {
            ftpClient._pasvSocket.destroy();
          }
        } catch (e) {
          // Ignorer les erreurs de fermeture silencieusement
          // Le socket peut déjà être fermé ou dans un état invalide
        }
      }
    }, delay);
  }

  get ftp() {
    return this.getClient().getFsClient();
  }

  toFileStat(stat): FileStats {
    const mtime = this.toLocalTime(stat.date.getTime());
    return {
      type: FTPFileSystem.getFileType(stat.type),
      mode: toNumMode(stat.rights), // Caution: windows will always get 0o666
      size: stat.size,
      mtime,
      atime: mtime,
      target: stat.target,
    };
  }

  toFileEntry(fullPath, stat): FileEntry {
    return {
      fspath: fullPath,
      name: stat.name,
      ...this.toFileStat(stat),
    };
  }

  _createClient(option) {
    return new FTPClient(option);
  }

  async lstat(path: string): Promise<FileStats> {
    if (path === '/') {
      return {
        type: FileType.Directory,
        mode: 0o666,
        size: 0,
        mtime: 0,
        atime: 0,
      };
    }

    const parentPath = this.pathResolver.dirname(path);
    const nameIdentity = this.pathResolver.basename(path);
    const stats = await this.list(parentPath);

    const fileStat = stats.find(ns => ns.name === nameIdentity);

    if (!fileStat) {
      throw new Error('file not exist');
    }

    return fileStat;
  }

  open(path: string, flags: string, mode?: number): Promise<FtpFileHandle> {
    return Promise.resolve({
      path,
      flags,
      mode,
    });
  }

  close(_fd: FtpFileHandle): Promise<void> {
    return Promise.resolve();
  }

  fstat(fd: FtpFileHandle): Promise<FileStats> {
    return this.lstat(fd.path);
  }

  futimes(fd: FtpFileHandle, _atime: number, mtime: number): Promise<void> {
    if (!this._supportMFMT) return Promise.resolve();

    return this.atomicSetLastMod(fd.path, new Date(mtime * 1000)).catch(_ => {
      logger.info('Don\'t Support MFMT');
      this._supportMFMT = false;
    });
  }

  async get(path, _option?: FileOption): Promise<Readable> {
    const stream = await this.atomicGet(path);

    if (!stream) {
      throw new Error('create ReadStream failed');
    }

    return stream;
  }

  async chmod(path: string, mode: number): Promise<void> {
    const command = `CHMOD ${mode.toString(8)} ${path}`;
    return await this.atomicSite(command);
  }

  async put(input: Readable, path, _option?: FileOption): Promise<void> {
    let inputError: Error | undefined;
    input.once('error', err => {
      inputError = err;
      this.ftp.abort(abortErr => {
        if (abortErr) {
          logger.error(abortErr, 'fail to abort');
        }
      });
    });

    try {
      await this.atomicPut(input, path);
    } catch (error) {
      throw inputError || error;
    }
  }

  readlink(path: string): Promise<string> {
    return this.lstat(path).then(stat => stat.target!);
  }

  symlink(_targetPath: string, _path: string): Promise<void> {
    // TO-DO implement
    return Promise.resolve();
  }

  async mkdir(dir: string): Promise<void> {
    return await this.atomicMakeDir(dir);
  }

  async ensureDir(dir: string): Promise<void> {
    return await this._ensureDir(dir, true);
  }

  async _ensureDir(dir: string, checkExistFirst: boolean): Promise<void> {
    // check if exist first.
    // `ls` command can't make sure to return dotfiles, so this not work for dotfiles,
    // cause ftp don't return distinct error code for dir not exists and dir exists
    if (checkExistFirst) {
      let stat;
      try {
        stat = await this.lstat(dir);
      } catch {
        // ignore error
      }

      if (stat) {
        if (stat.type !== FileType.Directory) {
          logger.error(`${dir} (type = ${stat.type})is not a directory`);
          throw new Error(`${dir} is not a valid directory path`);
        }

        return;
      }
    }

    let err;
    try {
      await this.mkdir(dir);
      return;
    } catch (error) {
      // avoid nested code block
      err = error;
    }

    switch (err.code) {
      case 550:
        // Hooray, exists!
        if (err.message.toLowerCase().indexOf('file exists') >= 0) {
          return;
        }

        const parentPath = this.pathResolver.dirname(dir);
        // We are trying to create the root dir, something must go wrong.
        if (parentPath === dir) {
          throw err;
        }

        // If goes here, we can assume the file doesn't exist
        await this._ensureDir(parentPath, false);
        await this.mkdir(dir);
        break;

      // In the case of any other error, just see if there's a dir
      // there already.  If so, then hooray!  If not, then something
      // is borked.
      default:
        try {
          const stat = await this.lstat(dir);
          if (stat.type !== FileType.Directory) throw err;
        } catch {
          // if the stat fails, then that's super weird.
          // let the original error be the failure reason
          throw err;
        }
        break;
    }
  }

  async list(
    dir: string,
    { showHiddenFiles = false } = {}
  ): Promise<FileEntry[]> {
    // -al flag only get partially support
    const stats = await this.atomicList(dir);

    return (
      stats
        // item will be a string if ftp fail to parse it (https://github.com/liximomo/vscode-sftp/issues/308)
        // we simply ignore it by check whether it has a name property
        .filter(item => item.name && item.name !== '.' && item.name !== '..')
        .map(item =>
          this.toFileEntry(this.pathResolver.join(dir, item.name), item)
        )
    );
  }

  async unlink(path: string): Promise<void> {
    return await this.atomicDeleteFile(path);
  }

  async rmdir(path: string, recursive: boolean): Promise<void> {
    return await this.atomicRemoveDir(path, recursive);
  }

  async rename(srcPath: string, destPath: string): Promise<void> {
    return await this.renameAtomic(srcPath, destPath);
  }

  async renameAtomic(srcPath: string, destPath: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const task = {
        run: () =>
          new Promise<void>((taskResolve, taskReject) => {
            this.ftp.rename(srcPath, destPath, err => {
              if (err) {
                taskReject(err);
                return reject(err);
              }

              resolve();
              taskResolve();
            });
          }),
      };

      this.queue.add(task);
    });
  }

  private async atomicList(path: string): Promise<any[]> {
    return new Promise<any[]>((resolve, reject) => {
      const task = {
        run: () =>
          new Promise<void>((taskResolve, taskReject) => {
            const ftpClient = this.ftp;
            ftpClient.list(path, (err, stats) => {
              // Fermer le socket passif après la liste, même en cas d'erreur
              this.closePasvSocket(ftpClient);

              if (err) {
                taskReject(err);
                return reject(err);
              }

              resolve(stats || []);
              taskResolve();
            });
          }),
      };

      this.queue.add(task);
    });
  }

  private async atomicGet(path: string): Promise<Readable> {
    return new Promise<Readable>((resolve, reject) => {
      const task = {
        run: () =>
          new Promise<void>((taskResolve, taskReject) => {
            const ftpClient = this.ftp;
            ftpClient.get(path, (err, stream) => {
              if (err) {
                // Fermer le socket passif même en cas d'erreur
                this.closePasvSocket(ftpClient);
                taskReject(err);
                return reject(err);
              }

              // Fermer le socket passif quand le stream se termine naturellement
              stream.once('end', () => {
                this.closePasvSocket(ftpClient);
              });

              stream.once('close', () => {
                this.closePasvSocket(ftpClient);
              });

              stream.once('error', () => {
                this.closePasvSocket(ftpClient);
              });

              resolve(stream);
              taskResolve();
            });
          }),
      };

      this.queue.add(task);
    });
  }

  private async atomicPut(input: Readable, path: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const task = {
        run: () =>
          new Promise<void>((taskResolve, taskReject) => {
            const ftpClient = this.ftp;
            ftpClient.put(input, path, err => {
              // Fermer le socket passif après le transfert, même en cas d'erreur
              this.closePasvSocket(ftpClient);

              if (err) {
                taskReject(err);
                return reject(err);
              }

              resolve();
              taskResolve();
            });
          }),
      };

      this.queue.add(task);
    });
  }

  private async atomicDeleteFile(path: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const task = {
        run: () =>
          new Promise<void>((taskResolve, taskReject) => {
            this.ftp.delete(path, err => {
              if (err) {
                taskReject(err);
                return reject(err);
              }

              resolve();
              taskResolve();
            });
          }),
      };

      this.queue.add(task);
    });
  }

  private async atomicMakeDir(path: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const task = {
        run: () =>
          new Promise<void>((taskResolve, taskReject) => {
            this.ftp.mkdir(path, err => {
              if (err) {
                taskReject(err);
                return reject(err);
              }

              resolve();
              taskResolve();
            });
          }),
      };

      this.queue.add(task);
    });
  }

  private async atomicRemoveDir(
    path: string,
    recursive: boolean
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const task = {
        run: () =>
          new Promise<void>((taskResolve, taskReject) => {
            this.ftp.rmdir(path, recursive, err => {
              if (err) {
                taskReject(err);
                return reject(err);
              }

              resolve();
              taskResolve();
            });
          }),
      };

      this.queue.add(task);
    });
  }

  private async atomicSite(command: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const task = {
        run: () =>
          new Promise<void>((taskResolve, taskReject) => {
            this.ftp.site(command, err => {
              if (err) {
                taskReject(err);
                return reject(err);
              }

              resolve();
              taskResolve();
            });
          }),
      };

      this.queue.add(task);
    });
  }

  private async atomicSetLastMod(path: string, date: Date): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const task = {
        run: () =>
          new Promise<void>((taskResolve, taskReject) => {
            this.ftp.setLastMod(path, date, err => {
              if (err) {
                taskReject(err);
                return reject(err);
              }

              resolve();
              taskResolve();
            });
          }),
      };

      this.queue.add(task);
    });
  }
}
