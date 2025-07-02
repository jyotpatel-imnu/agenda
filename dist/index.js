import { EventEmitter } from 'events';
import debug from 'debug';
import { Job } from './Job.js';
import { JobDbRepository } from './JobDbRepository.js';
import { parsePriority } from './utils/priority.js';
import { JobProcessor } from './JobProcessor.js';
import { calculateProcessEvery } from './utils/processEvery.js';
import { getCallerFilePath } from './utils/stack.js';
const log = debug('agenda');
const DefaultOptions = {
    processEvery: 5000,
    defaultConcurrency: 5,
    maxConcurrency: 20,
    defaultLockLimit: 0,
    lockLimit: 0,
    defaultLockLifetime: 10 * 60 * 1000,
    sort: { nextRunAt: 1, priority: -1 },
    forkHelper: { path: 'dist/childWorker.js' }
};
/**
 * @class
 */
export class Agenda extends EventEmitter {
    attrs;
    forkedWorker;
    forkHelper;
    db;
    on(event, listener) {
        if (this.forkedWorker && event !== 'ready' && event !== 'error') {
            const warning = new Error(`calling on(${event}) during a forkedWorker has no effect!`);
            console.warn(warning.message, warning.stack);
            return this;
        }
        return super.on(event, listener);
    }
    definitions = {};
    jobProcessor;
    ready;
    isActiveJobProcessor() {
        return !!this.jobProcessor;
    }
    async getForkedJob(jobId) {
        const jobData = await this.db.getJobById(jobId);
        if (!jobData) {
            throw new Error('db entry not found');
        }
        const job = new Job(this, jobData);
        return job;
    }
    async getRunningStats(fullDetails = false) {
        if (!this.jobProcessor) {
            throw new Error('agenda not running!');
        }
        return this.jobProcessor.getStatus(fullDetails);
    }
    /**
     * @param {Object} config - Agenda Config
     * @param {Function} cb - Callback after Agenda has started and connected to mongo
     */
    constructor(config = DefaultOptions, cb) {
        super();
        this.attrs = {
            name: config.name || '',
            processEvery: calculateProcessEvery(config.processEvery) || DefaultOptions.processEvery,
            defaultConcurrency: config.defaultConcurrency || DefaultOptions.defaultConcurrency,
            maxConcurrency: config.maxConcurrency || DefaultOptions.maxConcurrency,
            defaultLockLimit: config.defaultLockLimit || DefaultOptions.defaultLockLimit,
            lockLimit: config.lockLimit || DefaultOptions.lockLimit,
            defaultLockLifetime: config.defaultLockLifetime || DefaultOptions.defaultLockLifetime,
            sort: config.sort || DefaultOptions.sort
        };
        this.forkedWorker = config.forkedWorker;
        this.forkHelper = config.forkHelper;
        this.ready = new Promise(resolve => {
            this.once('ready', resolve);
        });
        if (this.hasDatabaseConfig(config)) {
            this.db = new JobDbRepository(this, config);
            this.db.connect();
        }
        if (cb) {
            this.ready.then(() => cb());
        }
    }
    /**
     * Connect to the spec'd MongoDB server and database.
     */
    async database(address, collection, options) {
        this.db = new JobDbRepository(this, { db: { address, collection, options } });
        await this.db.connect();
        return this;
    }
    /**
     * Use existing mongo connectino to pass into agenda
     * @param mongo
     * @param collection
     */
    async mongo(mongo, collection) {
        this.db = new JobDbRepository(this, { mongo, db: { collection } });
        await this.db.connect();
        return this;
    }
    /**
     * Set the sort query for finding next job
     * Default is { nextRunAt: 1, priority: -1 }
     * @param query
     */
    sort(query) {
        log('Agenda.sort([Object])');
        this.attrs.sort = query;
        return this;
    }
    hasDatabaseConfig(config) {
        return !!(config?.db?.address || config?.mongo);
    }
    /**
     * Cancels any jobs matching the passed MongoDB query, and removes them from the database.
     * @param query
     */
    async cancel(query) {
        log('attempting to cancel all Agenda jobs', query);
        try {
            const amountOfRemovedJobs = await this.db.removeJobs(query);
            log('%s jobs cancelled', amountOfRemovedJobs);
            return amountOfRemovedJobs;
        }
        catch (error) {
            log('error trying to delete jobs from MongoDB');
            throw error;
        }
    }
    /**
     * Set name of queue
     * @param name
     */
    name(name) {
        log('Agenda.name(%s)', name);
        this.attrs.name = name;
        return this;
    }
    /**
     * Set the time how often the job processor checks for new jobs to process
     * @param time
     */
    processEvery(time) {
        if (this.jobProcessor) {
            throw new Error('job processor is already running, you need to set processEvery before calling start');
        }
        log('Agenda.processEvery(%d)', time);
        this.attrs.processEvery = calculateProcessEvery(time);
        return this;
    }
    /**
     * Set the concurrency for jobs (globally), type does not matter
     * @param num
     */
    maxConcurrency(num) {
        log('Agenda.maxConcurrency(%d)', num);
        this.attrs.maxConcurrency = num;
        return this;
    }
    /**
     * Set the default concurrency for each job
     * @param num number of max concurrency
     */
    defaultConcurrency(num) {
        log('Agenda.defaultConcurrency(%d)', num);
        this.attrs.defaultConcurrency = num;
        return this;
    }
    /**
     * Set the default amount jobs that are allowed to be locked at one time (GLOBAL)
     * @param num
     */
    lockLimit(num) {
        log('Agenda.lockLimit(%d)', num);
        this.attrs.lockLimit = num;
        return this;
    }
    /**
     * Set default lock limit per job type
     * @param num
     */
    defaultLockLimit(num) {
        log('Agenda.defaultLockLimit(%d)', num);
        this.attrs.defaultLockLimit = num;
        return this;
    }
    /**
     * Set the default lock time (in ms)
     * Default is 10 * 60 * 1000 ms (10 minutes)
     * @param ms
     */
    defaultLockLifetime(ms) {
        log('Agenda.defaultLockLifetime(%d)', ms);
        this.attrs.defaultLockLifetime = ms;
        return this;
    }
    /**
     * Finds all jobs matching 'query'
     * @param query
     * @param sort
     * @param limit
     * @param skip
     */
    async jobs(query = {}, sort = {}, limit = 0, skip = 0) {
        const result = await this.db.getJobs(query, sort, limit, skip);
        return result.map(job => new Job(this, job));
    }
    /**
     * Removes all jobs from queue
     * @note: Only use after defining your jobs
     */
    async purge() {
        const definedNames = Object.keys(this.definitions);
        log('Agenda.purge(%o)', definedNames);
        return this.cancel({ name: { $not: { $in: definedNames } } });
    }
    define(name, processor, options) {
        if (this.definitions[name]) {
            log('overwriting already defined agenda job', name);
        }
        const filePath = getCallerFilePath();
        this.definitions[name] = {
            fn: processor,
            filePath,
            concurrency: options?.concurrency || this.attrs.defaultConcurrency,
            lockLimit: options?.lockLimit || this.attrs.defaultLockLimit,
            priority: parsePriority(options?.priority),
            lockLifetime: options?.lockLifetime || this.attrs.defaultLockLifetime
        };
        log('job [%s] defined with following options: \n%O', name, this.definitions[name]);
    }
    /**
     * Internal helper method that uses createJob to create jobs for an array of names
     * @param {Number} interval run every X interval
     * @param {Array<String>} names Strings of jobs to schedule
     * @param {Object} data data to run for job
     * @param {Object} options options to run job for
     * @returns {Array<Job>} array of jobs created
     */
    async createJobs(names, createJob) {
        try {
            const jobs = await Promise.all(names.map(name => createJob(name)));
            log('createJobs() -> all jobs created successfully');
            return jobs;
        }
        catch (error) {
            log('createJobs() -> error creating one or more of the jobs', error);
            throw error;
        }
    }
    create(name, data) {
        log('Agenda.create(%s, [Object])', name);
        const priority = this.definitions[name] ? this.definitions[name].priority : 0;
        const job = new Job(this, { name, data, type: 'normal', priority });
        return job;
    }
    async every(interval, names, data, options) {
        /**
         * Internal method to setup job that gets run every interval
         * @param {Number} interval run every X interval
         * @param {String} name String job to schedule
         * @param {Object} data data to run for job
         * @param {Object} options options to run job for
         * @returns {Job} instance of job
         */
        log('Agenda.every(%s, %O, %O)', interval, names, options);
        const createJob = async (name) => {
            const job = this.create(name, data);
            job.attrs.type = 'single';
            job.repeatEvery(interval, options);
            if (options?.forkMode) {
                job.forkMode(options.forkMode);
            }
            await job.save();
            return job;
        };
        if (typeof names === 'string') {
            const job = await createJob(names);
            return job;
        }
        log('Agenda.every(%s, %s, %O)', interval, names, options);
        const jobs = await this.createJobs(names, createJob);
        return jobs;
    }
    async schedule(when, names, data) {
        const createJob = async (name) => {
            const job = this.create(name, data);
            await job.schedule(when).save();
            return job;
        };
        if (typeof names === 'string') {
            log('Agenda.schedule(%s, %O, [%O])', when, names);
            return createJob(names);
        }
        log('Agenda.schedule(%s, %O, [%O])', when, names);
        return this.createJobs(names, createJob);
    }
    async now(name, data) {
        log('Agenda.now(%s, [Object])', name);
        try {
            const job = this.create(name, data);
            job.schedule(new Date());
            await job.save();
            return job;
        }
        catch (error) {
            log('error trying to create a job for this exact moment');
            throw error;
        }
    }
    /**
     * Starts processing jobs using processJobs() methods, storing an interval ID
     * This method will only resolve if a db has been set up beforehand.
     */
    async start() {
        log('Agenda.start called, waiting for agenda to be initialized (db connection)', this.attrs.processEvery);
        await this.ready;
        if (this.jobProcessor) {
            log('Agenda.start was already called, ignoring');
            return;
        }
        this.jobProcessor = new JobProcessor(this, this.attrs.maxConcurrency, this.attrs.lockLimit, this.attrs.processEvery);
        this.on('processJob', this.jobProcessor.process.bind(this.jobProcessor));
    }
    /**
     * Clear the interval that processes the jobs and unlocks all currently locked jobs
     */
    async stop() {
        if (!this.jobProcessor) {
            log('Agenda.stop called, but agenda has never started!');
            return;
        }
        log('Agenda.stop called, clearing interval for processJobs()');
        const lockedJobs = this.jobProcessor.stop();
        log('Agenda._unlockJobs()');
        lockedJobs?.forEach(job => job.cancel(new Error('agenda stopped')));
        const jobIds = lockedJobs?.map(job => job.attrs._id) || [];
        if (jobIds.length > 0) {
            log('about to unlock jobs with ids: %O', jobIds);
            await this.db.unlockJobs(jobIds);
        }
        this.off('processJob', this.jobProcessor.process.bind(this.jobProcessor));
        this.jobProcessor = undefined;
    }
}
export * from './types/AgendaConfig.js';
export * from './types/JobDefinition.js';
export * from './types/JobParameters.js';
export * from './types/DbOptions.js';
export * from './Job.js';
//# sourceMappingURL=index.js.map