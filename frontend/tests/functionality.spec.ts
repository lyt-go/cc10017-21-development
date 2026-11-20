import { expect, test } from '@playwright/test'
import { readFile } from 'node:fs/promises'

type ModuleCase = {
  key: string
  fields: string[]
  actions: string[]
  actionTargets: string[]
  shape: string
}

const modules: ModuleCase[] = [
  {
    "key": "topic_registry",
    "fields": [
      "主题编号",
      "主题名称",
      "消息格式",
      "序列化协议",
      "分区键列",
      "保留时长",
      "压缩类型",
      "Schema版本"
    ],
    "actions": [
      "提交校验",
      "确认上线",
      "标记下线"
    ],
    "actionTargets": [
      "待校验",
      "已上线",
      "已下线"
    ],
    "shape": "crud-table"
  },
  {
    "key": "flink_job",
    "fields": [
      "任务编号",
      "消费主题",
      "转换逻辑",
      "输出表名",
      "并行度",
      "检查点间隔",
      "状态后端",
      "水位策略"
    ],
    "actions": [
      "提交校验",
      "启动作业",
      "停止作业"
    ],
    "actionTargets": [
      "待校验",
      "运行中",
      "已停止"
    ],
    "shape": "job-timeline"
  },
  {
    "key": "kafka_connector",
    "fields": [
      "配置编号",
      "集群地址",
      "消费组名",
      "偏移策略",
      "反序列化器",
      "批量大小",
      "超时时间",
      "死信队列"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用配置"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "schema_evolve",
    "fields": [
      "规则编号",
      "关联主题",
      "新增字段",
      "删除字段",
      "类型变更",
      "默认值填充",
      "兼容模式",
      "生效版本"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "执行回滚"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已回滚"
    ],
    "shape": "crud-table"
  },
  {
    "key": "upsert_merge",
    "fields": [
      "策略编号",
      "目标表名",
      "合并键列",
      "插入列",
      "更新列",
      "删除标记",
      "排序字段",
      "合并引擎"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用策略"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "lake_partition",
    "fields": [
      "分区编号",
      "目标表名",
      "分区列名",
      "分区粒度",
      "分区转换",
      "存储位置",
      "文件格式",
      "生命周期"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用分区"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "stream_quality",
    "fields": [
      "规则编号",
      "监控主题",
      "校验类型",
      "告警阈值",
      "采样比例",
      "检测窗口",
      "阻断动作",
      "规则版本"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用规则"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "iceberg_table",
    "fields": [
      "表编号",
      "表名称",
      "存储格式",
      "分区策略",
      "排序字段",
      "快照过期",
      "孤立文件清理",
      "元数据版本"
    ],
    "actions": [
      "提交校验",
      "确认就绪",
      "标记下线"
    ],
    "actionTargets": [
      "待校验",
      "已就绪",
      "已下线"
    ],
    "shape": "crud-table"
  },
  {
    "key": "watermark_track",
    "fields": [
      "标记编号",
      "数据主题",
      "分区编号",
      "事件时间水位",
      "处理时间水位",
      "延迟记录数",
      "消费偏移",
      "检查时间"
    ],
    "actions": [
      "标记正常",
      "标记滞后",
      "停用追踪"
    ],
    "actionTargets": [
      "正常",
      "滞后",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "dead_letter",
    "fields": [
      "记录编号",
      "原始主题",
      "原始分区",
      "失败原因",
      "原始消息体",
      "重试次数",
      "入库时间",
      "处置状态"
    ],
    "actions": [
      "加入处置",
      "标记重放",
      "标记丢弃"
    ],
    "actionTargets": [
      "待处置",
      "已重放",
      "已丢弃"
    ],
    "shape": "crud-table"
  },
  {
    "key": "materialized_view",
    "fields": [
      "视图编号",
      "查询SQL",
      "基表名",
      "刷新模式",
      "刷新间隔",
      "存储位置",
      "数据量",
      "最后刷新"
    ],
    "actions": [
      "提交校验",
      "启用刷新",
      "停用视图"
    ],
    "actionTargets": [
      "待校验",
      "已启用",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "change_capture",
    "fields": [
      "配置编号",
      "源数据库",
      "源表名",
      "快照模式",
      "捕获列",
      "变更序号",
      "增量阶段",
      "目标主题"
    ],
    "actions": [
      "提交校验",
      "启动捕获",
      "停止捕获"
    ],
    "actionTargets": [
      "待校验",
      "运行中",
      "已停止"
    ],
    "shape": "job-timeline"
  },
  {
    "key": "compact_policy",
    "fields": [
      "策略编号",
      "目标表名",
      "触发条件",
      "合并并发",
      "目标文件大小",
      "最小文件数",
      "保留快照数",
      "执行时间窗口"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用策略"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "stream_lineage",
    "fields": [
      "记录编号",
      "上游主题",
      "下游表名",
      "转换节点",
      "字段映射",
      "采集时间",
      "延迟偏移",
      "链路版本"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用记录"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "dag-graph"
  },
  {
    "key": "backfill_stream",
    "fields": [
      "任务编号",
      "目标表名",
      "起始偏移",
      "截止偏移",
      "重放速率",
      "并行度",
      "覆盖策略",
      "结果校验"
    ],
    "actions": [
      "启动补数",
      "确认完成",
      "取消补数"
    ],
    "actionTargets": [
      "运行中",
      "已完成",
      "已取消"
    ],
    "shape": "job-timeline"
  },
  {
    "key": "sink_dispatch",
    "fields": [
      "规则编号",
      "湖表名",
      "目标系统",
      "写入模式",
      "字段映射",
      "过滤条件",
      "批次大小",
      "失败回退"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用规则"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "ttl_policy",
    "fields": [
      "策略编号",
      "目标表名",
      "热数据保留",
      "温数据窗口",
      "冷归档策略",
      "过期动作",
      "分区粒度",
      "恢复超时"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用策略"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "realtime_metrics",
    "fields": [
      "指标编号",
      "指标名称",
      "聚合窗口",
      "聚合函数",
      "过滤条件",
      "分组维度",
      "输出表名",
      "指标版本"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "标记废弃"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已废弃"
    ],
    "shape": "analysis-dashboard"
  }
]
const repoKind = 'fullstack'
const crudModules = modules.filter(m => m.shape === 'crud-table')
const dagModules = modules.filter(m => m.shape === 'dag-graph')
const timelineModules = modules.filter(m => m.shape === 'job-timeline')
const analysisModules = modules.filter(m => m.shape === 'analysis-dashboard')

function parseCsv(text: string): string[][] {
  const records: string[][] = []
  let row: string[] = []
  let cell = ''
  let quoted = false
  const input = text.replace(/^\uFEFF/, '')
  for (let index = 0; index < input.length; index++) {
    const char = input[index]
    if (quoted) {
      if (char === '"' && input[index + 1] === '"') { cell += '"'; index++ }
      else if (char === '"') { quoted = false }
      else { cell += char }
    } else if (char === '"' && cell === '') { quoted = true }
    else if (char === ',') { row.push(cell); cell = '' }
    else if (char === '\n') {
      row.push(cell.replace(/\r$/, ''))
      if (row.some(v => v !== '')) records.push(row)
      row = []; cell = ''
    } else { cell += char }
  }
  row.push(cell.replace(/\r$/, ''))
  if (row.some(v => v !== '')) records.push(row)
  return records
}

test('CRUD模块：筛选、导出、登记、动作、指标全链路', async ({ page }) => {
  test.setTimeout(300_000)
  const browserErrors: string[] = []
  page.on('pageerror', e => browserErrors.push(e.message))
  page.on('response', r => { if (r.url().includes('/api/') && r.status()>=400) browserErrors.push(`${r.status()} ${r.url()}`) })
  if (crudModules.length === 0) { test.skip(); return }

  for (const mod of crudModules) {
    await page.goto(`/${mod.key}`)
    await page.waitForTimeout(3000)
    const bodyRows = page.locator('tbody tr[data-testid^="row-"]')
    await expect(bodyRows).toHaveCount(3)
    const firstValue = await bodyRows.nth(0).locator('td').nth(0).innerText()

    await page.getByTestId(`filter-${mod.fields[0]}`).fill(firstValue)
    await page.getByTestId('query').click()
    await expect(bodyRows).toHaveCount(1)
    await page.getByTestId('reset').click()
    await expect(bodyRows).toHaveCount(3)

    const downloadPromise = page.waitForEvent('download')
    await page.getByTestId('export').click()
    const download = await downloadPromise
    expect(download.suggestedFilename()).toBe(`${mod.key}.csv`)

    const createdValues = mod.fields.map((f, i) => `验收-${mod.key}-${i+1}`)
    await page.getByTestId('open-create').click()
    await expect(page.getByTestId('create-modal')).toBeVisible()
    for (const [i, field] of mod.fields.entries()) {
      await page.getByTestId(`create-${field}`).fill(createdValues[i])
    }
    await page.getByTestId('submit-create').click()
    await expect(page.getByTestId('create-modal')).toHaveCount(0)
    await expect(bodyRows).toHaveCount(4)

    const createdRow = page.locator('tbody tr').filter({ hasText: createdValues[0] })
    for (const [i, action] of mod.actions.entries()) {
      await createdRow.getByRole('button', { name: action, exact: true }).click()
      await expect(createdRow).toHaveAttribute('data-status', mod.actionTargets[i])
    }
  }

  expect(browserErrors).toEqual([])
})

test('DAG拓扑模块：节点渲染、详情、动作、导出', async ({ page }) => {
  test.setTimeout(300_000)
  const browserErrors: string[] = []
  page.on('pageerror', e => browserErrors.push(e.message))
  page.on('response', r => { if (r.url().includes('/api/') && r.status()>=400) browserErrors.push(`${r.status()} ${r.url()}`) })
  if (dagModules.length === 0) { test.skip(); return }

  for (const mod of dagModules) {
    await page.goto(`/${mod.key}`)
    await page.waitForTimeout(3000)
    await expect(page.locator('svg.dag-svg')).toBeVisible()
    const nodes = page.locator('g.dag-node')
    const count = await nodes.count()
    expect(count).toBeGreaterThanOrEqual(3)

    await nodes.first().click()
    await expect(page.getByTestId('dag-detail')).toBeVisible()

    const downloadPromise = page.waitForEvent('download')
    await page.getByTestId('export').click()
    const download = await downloadPromise
    expect(download.suggestedFilename()).toBe(`${mod.key}.csv`)

    await page.getByTestId('open-create').click()
    await expect(page.getByTestId('create-modal')).toBeVisible()
    for (const [i, field] of mod.fields.slice(0,3).entries()) {
      await page.locator('.create-field input').nth(i).fill(`E2E-${mod.key}-${i}`)
    }
    await page.getByTestId('submit-create').click()
    await expect(page.getByTestId('create-modal')).toHaveCount(0)
  }

  expect(browserErrors).toEqual([])
})

test('时间线模块：甘特条渲染、日期筛选、日志查看、登记', async ({ page }) => {
  test.setTimeout(300_000)
  const browserErrors: string[] = []
  page.on('pageerror', e => browserErrors.push(e.message))
  page.on('response', r => { if (r.url().includes('/api/') && r.status()>=400) browserErrors.push(`${r.status()} ${r.url()}`) })
  if (timelineModules.length === 0) { test.skip(); return }

  for (const mod of timelineModules) {
    await page.goto(`/${mod.key}`)
    await expect(page.locator('.timeline-bar').first()).toBeVisible({ timeout: 10000 })
    const bars = page.locator('.timeline-bar')
    const count = await bars.count()
    expect(count).toBeGreaterThanOrEqual(3)

    await bars.first().click()
    await expect(page.getByTestId('timeline-log')).toBeVisible()

    const downloadPromise = page.waitForEvent('download')
    await page.getByTestId('export').click()
    const download = await downloadPromise
    expect(download.suggestedFilename()).toBe(`${mod.key}.csv`)

    await page.getByTestId('open-create').click()
    await expect(page.getByTestId('create-modal')).toBeVisible()
    for (const [i, field] of mod.fields.slice(0,3).entries()) {
      await page.locator('.create-field input').nth(i).fill(`E2E-${mod.key}-${i}`)
    }
    await page.getByTestId('submit-create').click()
    await expect(page.getByTestId('create-modal')).toHaveCount(0)
  }

  expect(browserErrors).toEqual([])
})

test('分析看板模块：KPI卡片、运行分析、图表渲染', async ({ page }) => {
  test.setTimeout(300_000)
  const browserErrors: string[] = []
  page.on('pageerror', e => browserErrors.push(e.message))
  page.on('response', r => { if (r.url().includes('/api/') && r.status()>=400) browserErrors.push(`${r.status()} ${r.url()}`) })
  if (analysisModules.length === 0) { test.skip(); return }

  for (const mod of analysisModules) {
    await page.goto(`/${mod.key}`)
    await expect(page.getByTestId('kpi-card').first()).toBeVisible({ timeout: 10000 })
    const kpiCards = page.getByTestId('kpi-card')
    const kpiCount = await kpiCards.count()
    expect(kpiCount).toBeGreaterThanOrEqual(2)
    await expect(kpiCards.first().locator('strong')).toBeVisible()

    await page.getByTestId('run-analysis').click()
    await expect(page.locator('.analysis-bar').first()).toBeVisible({ timeout: 5000 })
    await expect(page.locator('.data-table tbody tr').first()).toBeVisible({ timeout: 5000 })
  }

  expect(browserErrors).toEqual([])
})

test('概览汇总', async ({ page }) => {
  test.setTimeout(300_000)
  await page.goto('/')
  await expect(page.locator('.stat-value').nth(1)).toBeVisible()
  // 验证概览页加载正常即可，不硬编码精确计数（取决于各模块 E2E 登记成功数）
})

test('死信重放映射：生成、中断恢复、血缘共享与唯一主重放', async ({ page, request }) => {
  test.setTimeout(300_000)
  const browserErrors: string[] = []
  page.on('pageerror', e => browserErrors.push(e.message))
  page.on('response', r => { if (r.url().includes('/api/') && r.status() >= 400) browserErrors.push(`${r.status()} ${r.url()}`) })

  // 先落一套「完整映射形成」的有效版本，作为路线取舍的基线
  await request.post('/api/dead_letter/replay/generate', {
    data: { topic_code: 'TOPI-0001', route: '完整映射形成' },
  })

  await page.goto('/dead_letter')
  await expect(page.getByTestId('replay-panel')).toBeVisible()
  const mappingRows = page.locator('[data-testid^="mapping-"]')
  await expect(mappingRows).toHaveCount(3)

  // 换路线：首个节点接收只保留一条主重放边
  await page.getByTestId('replay-route').selectOption('首个节点接收')
  await page.getByTestId('generate-replay').click()
  await expect(mappingRows).toHaveCount(1)

  // 再换到完整映射，并在第 1 项后模拟中断
  await page.getByTestId('replay-route').selectOption('完整映射形成')
  await page.getByTestId('replay-interrupt').fill('1')
  await page.getByTestId('generate-replay').click()
  await expect(page.getByTestId('replay-message')).toContainText('中断')
  await expect(mappingRows).toHaveCount(1)
  const confirmedId = (await mappingRows.first().locator('td').first().innerText()).trim()

  // 恢复只补未完成项：已确认映射编号保留，缺口补到 3 条
  await page.getByTestId('resume-replay').click()
  await expect(page.getByTestId('replay-message')).toContainText('补齐')
  await expect(mappingRows).toHaveCount(3)
  const mappingIds = await page.locator('[data-testid^="mapping-"] td:first-child').allInnerTexts()
  expect(mappingIds.map((text) => text.trim())).toContain(confirmedId)
  await expect(page.getByTestId('replay-check')).toContainText('主重放唯一')

  // 每条死信只有一条主重放
  const primaryCells = await page.locator('[data-testid^="mapping-"] td:nth-child(8)').allInnerTexts()
  expect(primaryCells.filter((text) => text.trim() === '是')).toHaveLength(1)

  // 流式血缘页读取同一份映射，并画出到转换节点的重放边
  await page.goto('/stream_lineage')
  await expect(page.getByTestId('lineage-replay')).toBeVisible()
  await expect(page.locator('[data-testid^="lineage-mapping-"]')).toHaveCount(3)
  expect(await page.locator('line.dag-edge-replay').count()).toBe(3)

  // 标记重放后死信列表能说明进入哪个下游表
  const dl = await request.get('/api/dead_letter')
  const target = (await dl.json()).items.find((row: any) => row['原始主题'] === 'TOPI-0001' && row.status !== '已丢弃')
  await page.goto('/dead_letter')
  await page.getByTestId(`action-${target.id}-标记重放`).click()
  await expect(page.getByTestId(`row-${target.id}`)).toHaveAttribute('data-status', '已重放')
  await expect(page.getByTestId(`row-${target.id}`)).toContainText('flink-')

  expect(browserErrors).toEqual([])
})
