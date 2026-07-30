#!/usr/bin/env node
import { execFileSync, fork, spawnSync } from 'node:child_process'
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { Worker } from 'node:worker_threads'

const INTERNAL_ENV = 'ORCA_HANG_WATCHDOG_BENCH_INTERNAL'
const BOUNDARY_ENV = 'ORCA_HANG_WATCHDOG_BENCH_BOUNDARY'
const RESULT_PREFIX = 'ORCA_HANG_WATCHDOG_BENCH_RESULT='
const DEFAULT_TRIALS = 7
const SETTLE_MS = 2_000
const SAMPLE_COUNT = 5
const SAMPLE_INTERVAL_MS = 200
const VERIFY_TIMEOUT_MS = 500
const VERIFY_CHECK_INTERVAL_MS = 50
const VERIFY_BLOCK_MS = 1_200
const MIB = 1024 * 1024
const scriptPath = import.meta.filename
const repoRoot = path.resolve(import.meta.dirname, '..', '..')
const entryPath = path.join(repoRoot, 'out', 'main', 'main-thread-hang-watchdog-entry.js')

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function median(values) {
  const sorted = [...values].sort((left, right) => left - right)
  return sorted[Math.floor(sorted.length / 2)]
}

function forceGc() {
  if (typeof global.gc !== 'function') {
    throw new Error('Electron did not expose GC; keep --js-flags=--expose-gc in the harness')
  }
  global.gc()
  global.gc()
}

async function sampleRss(readRss) {
  const samples = []
  for (let index = 0; index < SAMPLE_COUNT; index += 1) {
    samples.push(readRss())
    await sleep(SAMPLE_INTERVAL_MS)
  }
  return median(samples)
}

function childRssBytes(pid) {
  const raw = execFileSync('ps', ['-o', 'rss=', '-p', String(pid)], {
    encoding: 'utf8'
  }).trim()
  const rssKiB = Number(raw)
  if (!Number.isFinite(rssKiB) || rssKiB <= 0) {
    throw new Error(`Could not read watchdog child RSS for PID ${pid}`)
  }
  return rssKiB * 1024
}

function blockMainThread(ms) {
  const startedAt = Date.now()
  while (Date.now() - startedAt < ms) {
    // Intentional synchronous stall.
  }
  return { startedAt, endedAt: Date.now() }
}

function readMarker(markerPath) {
  try {
    return JSON.parse(readFileSync(markerPath, 'utf8'))
  } catch {
    return null
  }
}

async function verifyBlockedMainDetection(markerPath, sendHeartbeat) {
  const block = blockMainThread(VERIFY_BLOCK_MS)
  const detected = readMarker(markerPath)
  sendHeartbeat()
  await sleep(VERIFY_CHECK_INTERVAL_MS * 4)
  const resolved = readMarker(markerPath)
  const verified =
    detected?.detectedAt >= block.startedAt &&
    detected.detectedAt <= block.endedAt &&
    detected.selfRecovered === false &&
    resolved?.selfRecovered === true
  if (!verified) {
    throw new Error(
      `Built watchdog failed blocked-main verification: ${JSON.stringify({ detected, resolved })}`
    )
  }
  return true
}

async function measureChild(markerPath) {
  const child = fork(entryPath, [], {
    stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
    env: {
      ...process.env,
      ELECTRON_RUN_AS_NODE: '1',
      ORCA_HANG_WATCHDOG_PARENT_PID: String(process.pid),
      ORCA_HANG_WATCHDOG_MARKER_PATH: markerPath,
      ORCA_HANG_WATCHDOG_TIMEOUT_MS: String(VERIFY_TIMEOUT_MS),
      ORCA_HANG_WATCHDOG_CHECK_INTERVAL_MS: String(VERIFY_CHECK_INTERVAL_MS)
    }
  })
  const sendHeartbeat = () => child.send?.({ type: 'heartbeat' })
  const heartbeat = setInterval(sendHeartbeat, 100)
  try {
    await sleep(SETTLE_MS)
    const rssBytes = await sampleRss(() => childRssBytes(child.pid))
    const blockedMainThreadVerified = await verifyBlockedMainDetection(markerPath, sendHeartbeat)
    return { rssBytes, blockedMainThreadVerified }
  } finally {
    clearInterval(heartbeat)
    child.send?.({ type: 'shutdown' })
    child.disconnect()
  }
}

async function measureWorker(markerPath) {
  forceGc()
  await sleep(SETTLE_MS)
  forceGc()
  const before = await sampleRss(() => process.memoryUsage().rss)
  const worker = new Worker(entryPath, {
    workerData: {
      parentPid: process.pid,
      markerPath,
      timeoutMs: VERIFY_TIMEOUT_MS,
      checkIntervalMs: VERIFY_CHECK_INTERVAL_MS
    }
  })
  const sendHeartbeat = () => worker.postMessage({ type: 'heartbeat' })
  const heartbeat = setInterval(sendHeartbeat, 100)
  try {
    await sleep(SETTLE_MS)
    forceGc()
    const after = await sampleRss(() => process.memoryUsage().rss)
    const blockedMainThreadVerified = await verifyBlockedMainDetection(markerPath, sendHeartbeat)
    return { rssBytes: Math.max(0, after - before), blockedMainThreadVerified }
  } finally {
    clearInterval(heartbeat)
    worker.postMessage({ type: 'shutdown' })
    await new Promise((resolve) => worker.once('exit', resolve))
  }
}

async function runInternal() {
  const { app } = await import('electron')
  const boundary = process.env[BOUNDARY_ENV]
  const profileDir = mkdtempSync(path.join(tmpdir(), 'orca-watchdog-bench-'))
  app.setPath('userData', profileDir)
  try {
    await app.whenReady()
    const markerPath = path.join(profileDir, 'main-thread-hang.json')
    const result =
      boundary === 'child'
        ? await measureChild(markerPath)
        : boundary === 'worker'
          ? await measureWorker(markerPath)
          : (() => {
              throw new Error(`Unsupported boundary: ${boundary}`)
            })()
    process.stdout.write(`${RESULT_PREFIX}${JSON.stringify(result)}\n`)
  } finally {
    app.quit()
    rmSync(profileDir, { recursive: true, force: true })
  }
}

function parseArgs(argv) {
  const options = { boundary: '', trials: DEFAULT_TRIALS, output: '' }
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]
    const value = argv[index + 1]
    if (arg === '--boundary' || arg === '--trials' || arg === '--output') {
      if (!value) {
        throw new Error(`Missing value for ${arg}`)
      }
      options[arg.slice(2)] = arg === '--trials' ? Number(value) : value
      index += 1
    } else {
      throw new Error(`Unknown argument: ${arg}`)
    }
  }
  if (!['child', 'worker'].includes(options.boundary)) {
    throw new Error('--boundary must be child or worker')
  }
  if (!Number.isInteger(options.trials) || options.trials < 1) {
    throw new Error('--trials must be a positive integer')
  }
  return options
}

function electronPath() {
  const requirePath = import.meta.resolve('electron')
  const electronModulePath = fileURLToPath(requirePath)
  return execFileSync(
    process.execPath,
    ['-e', `process.stdout.write(require(${JSON.stringify(electronModulePath)}))`],
    {
      encoding: 'utf8'
    }
  )
}

function runTrial(executable, boundary) {
  const env = { ...process.env, [INTERNAL_ENV]: '1', [BOUNDARY_ENV]: boundary }
  delete env.ELECTRON_RUN_AS_NODE
  const launcherDir = mkdtempSync(path.join(tmpdir(), 'orca-watchdog-bench-launcher-'))
  writeFileSync(
    path.join(launcherDir, 'package.json'),
    JSON.stringify({ name: 'orca-watchdog-benchmark', main: 'main.cjs' })
  )
  writeFileSync(
    path.join(launcherDir, 'main.cjs'),
    `import(${JSON.stringify(pathToFileURL(scriptPath).href)}).catch((error) => {
  console.error(error)
  process.exitCode = 1
})\n`
  )
  let result
  try {
    result = spawnSync(executable, ['--js-flags=--expose-gc', launcherDir], {
      cwd: repoRoot,
      env,
      encoding: 'utf8',
      timeout: 30_000
    })
  } finally {
    rmSync(launcherDir, { recursive: true, force: true })
  }
  if (result.status !== 0) {
    throw new Error(
      `Electron trial failed (${result.error?.message ?? result.signal ?? result.status}):\n` +
        `${result.stderr || result.stdout}`
    )
  }
  const line = result.stdout.split('\n').find((candidate) => candidate.startsWith(RESULT_PREFIX))
  if (!line) {
    throw new Error(`Electron trial did not report a result:\n${result.stdout}`)
  }
  return JSON.parse(line.slice(RESULT_PREFIX.length))
}

function runBenchmark() {
  if (process.platform !== 'darwin') {
    throw new Error('The production watchdog is macOS-only; run this benchmark on macOS')
  }
  if (!existsSync(entryPath)) {
    throw new Error(`Missing ${entryPath}; run pnpm exec electron-vite build first`)
  }
  const options = parseArgs(process.argv.slice(2))
  const executable = electronPath()
  const results = Array.from({ length: options.trials }, () =>
    runTrial(executable, options.boundary)
  )
  const rssBytes = results.map((result) => result.rssBytes)
  const report = {
    benchmark: 'hang-watchdog-memory',
    boundary: options.boundary,
    revision: execFileSync('git', ['rev-parse', 'HEAD'], {
      cwd: repoRoot,
      encoding: 'utf8'
    }).trim(),
    electron: execFileSync(executable, ['-e', 'process.stdout.write(process.versions.electron)'], {
      env: { ...process.env, ELECTRON_RUN_AS_NODE: '1' },
      encoding: 'utf8'
    }).trim(),
    settleMs: SETTLE_MS,
    samplesPerTrial: SAMPLE_COUNT,
    trials: options.trials,
    rssMiB: rssBytes.map((value) => Number((value / MIB).toFixed(2))),
    medianRssMiB: Number((median(rssBytes) / MIB).toFixed(2)),
    blockedMainThreadVerified: results.every((result) => result.blockedMainThreadVerified)
  }
  const serialized = `${JSON.stringify(report, null, 2)}\n`
  process.stdout.write(serialized)
  if (options.output) {
    writeFileSync(path.resolve(options.output), serialized)
  }
}

if (process.env[INTERNAL_ENV] === '1') {
  await runInternal()
} else {
  runBenchmark()
}
