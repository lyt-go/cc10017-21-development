<template>
  <section class="page" data-module="dead_letter">
    <header class="page-head">
      <div>
        <h2>死信处理管理</h2>
        <p class="page-desc">维护反序列化、校验与转换失败的异常消息隔离与回处理，并生成可追溯的死信重放映射数据。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记死信记录</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出死信处理清单</button>
        <button class="btn" data-testid="package" type="button" @click="downloadPackage">打包下载结果</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>重放转换节点</th>
          <th>重放下游表名</th>
          <th>重放映射状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row['重放转换节点'] || '—' }}</td>
          <td>{{ row['重放下游表名'] || '—' }}</td>
          <td>{{ row['重放映射状态'] || '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 4" class="empty-state">暂无死信处理数据，可先登记死信记录</td>
        </tr>
      </tbody>
    </table>

    <section class="replay-panel" data-testid="replay-panel">
      <header class="replay-head">
        <div>
          <h3>死信重放映射</h3>
          <p class="page-desc">
            解析订单主题死信并保留上游分区到下游转换节点的映射；映射与血缘边成对写入，
            中断后再次生成只补齐未完成项，每条死信仅保留一个主重放入口；换处理路线会整体收口出新版本。
          </p>
        </div>
        <div class="replay-check" data-testid="replay-check">
          <span>版本 <strong>{{ check.version ?? '—' }}</strong></span>
          <span>计划 <strong>{{ check['计划'] ?? 0 }}</strong></span>
          <span>已确认 <strong>{{ check['已确认'] ?? 0 }}</strong></span>
          <span>待补齐 <strong>{{ check['待补齐'] ?? 0 }}</strong></span>
          <span :class="check.primary_ok === false ? 'error-text' : ''">
            主重放唯一 <strong>{{ check.primary_ok === false ? '否' : '是' }}</strong>
          </span>
          <span :class="check.atomic_ok === false ? 'error-text' : ''">
            映射边配对 <strong>{{ check.atomic_ok === false ? '否' : '是' }}</strong>
          </span>
        </div>
      </header>

      <div class="filter-bar">
        <label class="filter-item">
          <span>原始主题</span>
          <input v-model="replayForm.topic_code" data-testid="replay-topic" placeholder="如 TOPI-0001" />
        </label>
        <label class="filter-item">
          <span>处理路线</span>
          <select v-model="replayForm.route" data-testid="replay-route">
            <option v-for="route in routes" :key="route" :value="route">{{ route }}</option>
          </select>
        </label>
        <label class="filter-item">
          <span>中断位置（仅下一次生成/恢复生效）</span>
          <input v-model="replayForm.interrupt_after" data-testid="replay-interrupt" type="number" min="0" placeholder="如 1" />
        </label>
        <button class="btn primary" data-testid="generate-replay" type="button" @click="generateReplay">生成重放映射</button>
        <button class="btn" data-testid="resume-replay" type="button" @click="resumeReplay()">恢复中断批次</button>
        <button class="btn" data-testid="check-replay" type="button" @click="loadReplay">重新校验</button>
        <button class="btn" data-testid="export-replay" type="button" @click="exportMappings">导出重放清单</button>
        <label class="btn" style="cursor:pointer">
          上传清单
          <input data-testid="import-replay" type="file" accept=".csv,.zip,text/csv,application/zip" style="display:none" @change="importMappings" />
        </label>
      </div>
      <p v-if="replayMessage" class="replay-message" data-testid="replay-message">{{ replayMessage }}</p>

      <h4>重放批次</h4>
      <table class="data-table">
        <thead>
          <tr><th>批次编号</th><th>处理路线</th><th>版本</th><th>状态</th><th>计划数</th><th>已确认数</th><th>中断位置</th><th>更新时间</th><th>操作</th></tr>
        </thead>
        <tbody>
          <tr v-for="batch in batches" :key="String(batch.id)" :data-testid="`batch-${batch.id}`">
            <td>{{ batch['批次编号'] }}</td>
            <td>{{ batch['处理路线'] }}</td>
            <td>{{ batch['版本'] }}</td>
            <td>{{ batch['状态'] }}</td>
            <td>{{ batch['计划数'] }}</td>
            <td>{{ batch['已确认数'] }}</td>
            <td>{{ batch['中断位置'] ?? '—' }}</td>
            <td>{{ batch['更新时间'] }}</td>
            <td>
              <button v-if="batch['状态'] === '已中断'" class="link" type="button" @click="resumeReplay(Number(batch.id))">恢复该批次</button>
              <span v-else>—</span>
            </td>
          </tr>
          <tr v-if="!batches.length"><td colspan="9" class="empty-state">暂无重放批次</td></tr>
        </tbody>
      </table>

      <h4>当前有效映射（版本 {{ check.version ?? '—' }}）</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>映射编号</th><th>版本</th><th>死信编号</th><th>原始主题</th><th>原始分区</th>
            <th>转换节点</th><th>下游表名</th><th>主重放</th><th>映射状态</th><th>解析结果</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in mappings" :key="String(item.id)" :data-testid="`mapping-${item.id}`">
            <td>{{ item['映射编号'] }}</td>
            <td>{{ item['版本'] }}</td>
            <td>{{ item['死信编号'] }}</td>
            <td>{{ item['原始主题'] }}</td>
            <td>{{ item['原始分区'] }}</td>
            <td>{{ item['转换节点'] }}</td>
            <td>{{ item['下游表名'] }}</td>
            <td>{{ item['主重放'] ? '是' : '否' }}</td>
            <td>{{ item['映射状态'] }}</td>
            <td>{{ item['解析结果'] || '—' }}</td>
          </tr>
          <tr v-if="!mappings.length"><td colspan="10" class="empty-state">暂无重放映射，可对订单主题死信生成映射</td></tr>
        </tbody>
      </table>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记死信记录</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条死信处理记录 · 当前版本 {{ check.version ?? 0 }} 有 {{ check['已确认'] ?? 0 }} 条有效映射</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null | boolean>

const ENDPOINT = '/api/dead_letter'
const columns = ["记录编号", "原始主题", "原始分区", "失败原因", "原始消息体", "重试次数", "入库时间", "处置状态"]
const actions = ["加入处置", "标记重放", "标记丢弃"]
const statuses = ["草稿", "待处置", "已重放", "已丢弃"]
const routes = ["首个节点接收", "完整映射形成"]
const metricNames = ["死信记录总数", "待处置记录", "已重放记录", "有效重放映射"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})

const replayForm = ref({ topic_code: 'TOPI-0001', route: routes[0], interrupt_after: '' })
const replayMessage = ref('')
const check = ref<Record<string, any>>({})
const batches = ref<Row[]>([])
const mappings = ref<Row[]>([])

const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[2]).length },
  { label: metricNames[3], value: check.value['已确认'] ?? mappings.value.length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'dead_letter.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function exportMappings() {
  window.open(`${ENDPOINT}/replay/export`, '_blank')
}

function downloadPackage() {
  window.open(`${ENDPOINT}/package`, '_blank')
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('死信处理动作未生效，请稍后重试')
    }
    const payload = await response.json()
    if (payload.message) replayMessage.value = String(payload.message)
    await reload()
    await loadReplay()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '死信处理操作失败'
  }
}

function interruptValue(): number | null {
  const raw = String(replayForm.value.interrupt_after ?? '').trim()
  if (!raw) return null
  const value = Number(raw)
  return Number.isFinite(value) && value >= 0 ? Math.floor(value) : null
}

async function generateReplay() {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/replay/generate`, {
      method: 'POST',
      body: JSON.stringify({
        topic_code: replayForm.value.topic_code,
        route: replayForm.value.route,
        interrupt_after: interruptValue(),
      }),
    })
    const payload = await response.json()
    replayMessage.value = payload.message || ''
    if (!payload.ok) errorMessage.value = payload.message || '生成重放映射失败'
    // 中断位置只作用于本次调用，避免下一次恢复沿用旧值而反复中断。
    replayForm.value.interrupt_after = ''
    await reload()
    await loadReplay()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '生成重放映射失败'
  }
}

async function resumeReplay(batchId?: number) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/replay/resume`, {
      method: 'POST',
      body: JSON.stringify({ batch_id: batchId ?? null, interrupt_after: interruptValue() }),
    })
    const payload = await response.json()
    replayMessage.value = payload.message || ''
    if (!payload.ok) errorMessage.value = payload.message || '恢复重放映射失败'
    replayForm.value.interrupt_after = ''
    await reload()
    await loadReplay()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复重放映射失败'
  }
}

async function importMappings(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file) return
  errorMessage.value = ''
  try {
    const body = await file.arrayBuffer()
    const response = await request(
      `${ENDPOINT}/replay/import?route=${encodeURIComponent(replayForm.value.route)}`,
      { method: 'POST', body, headers: { 'Content-Type': 'application/octet-stream' } },
    )
    const payload = await response.json()
    replayMessage.value = payload.message || ''
    if (!payload.ok) errorMessage.value = payload.message || '上传清单未写入'
    await reload()
    await loadReplay()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '上传清单失败'
  } finally {
    input.value = ''
  }
}

async function loadReplay() {
  try {
    const [checkRes, batchRes, mappingRes] = await Promise.all([
      request(`${ENDPOINT}/replay/check`),
      request(`${ENDPOINT}/replay/batches`),
      request(`${ENDPOINT}/replay/mappings?size=200`),
    ])
    check.value = await checkRes.json()
    batches.value = (await batchRes.json()).items ?? []
    mappings.value = (await mappingRes.json()).items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重放映射读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('死信记录列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '死信处理列表读取失败'
  }
}

onMounted(async () => {
  await reload()
  await loadReplay()
})
</script>

<style scoped>
.replay-panel { margin-top: 20px; padding: 14px; background: #fff; border: 1px solid var(--border); border-radius: 8px; }
.replay-head { display: flex; justify-content: space-between; gap: 16px; flex-wrap: wrap; align-items: flex-start; }
.replay-head h3 { margin: 0; font-size: 15px; }
.replay-check { display: flex; flex-wrap: wrap; gap: 12px; font-size: 12px; color: var(--muted); }
.replay-check strong { color: #1f2937; }
.replay-panel h4 { margin: 16px 0 8px; font-size: 13px; color: var(--muted); }
.replay-message { margin: 4px 0 0; font-size: 12px; color: var(--brand); }
</style>
