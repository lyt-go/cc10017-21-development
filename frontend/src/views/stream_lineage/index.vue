<template>
  <section class="page" data-module="stream_lineage">
    <header class="page-head">
      <div><h2>流式血缘</h2><p class="page-desc">维护流式数据从消息到湖表的全链路血缘与延迟追踪，并展示死信重放形成的上游分区到下游转换节点边。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记血缘记录</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出关系清单</button>
      </div>
    </header>
    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#94a3b8"/></marker></defs>
        <g v-for="(edge, index) in graph.edges" :key="`${edge.from}-${edge.to}-${index}`">
          <line class="dag-edge" :class="{ 'dag-edge-replay': edge.label === '死信重放' }" :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0" :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"/>
          <text :x="((nodePos[edge.from]?.x||0)+(nodePos[edge.to]?.x||0))/2" :y="((nodePos[edge.from]?.y||0)+(nodePos[edge.to]?.y||0))/2 - 6" font-size="10" fill="#64748b" text-anchor="middle">{{ edge.label }}</text>
        </g>
        <g v-for="node in graph.nodes" :key="node.id" :class="['dag-node', {selected: selectedNode?.id===node.id}]" @click="selectNode(node)" @mousedown="startDrag($event, node)">
          <rect :x="(nodePos[node.id]?.x||0) - 60" :y="(nodePos[node.id]?.y||0) - 20" width="120" height="40" rx="6" :fill="nodeColor(node.status)"/>
          <text :x="nodePos[node.id]?.x||0" :y="(nodePos[node.id]?.y||0) + 4" text-anchor="middle">{{ node.name || node.label }}</text>
        </g>
      </svg>
    </div>
    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>编号</dt><dd>{{ selectedNode.id }}</dd></dl>
      <dl><dt>名称</dt><dd>{{ selectedNode.name || selectedNode.label }}</dd></dl>
      <dl v-for="f in detailFields" :key="f"><dt>{{ f }}</dt><dd>{{ selectedNode[f] ?? '—' }}</dd></dl>
      <div style="grid-column:1/-1; margin-top:8px; display:flex; gap:6px;">
        <button v-for="action in actions" :key="action" class="btn" :disabled="!isLineageNode(selectedNode)" :data-testid="`action-${selectedNode.id}-${action}`" @click="runAction(action, selectedNode)">{{ action }}</button>
      </div>
    </div>

    <section class="replay-panel" data-testid="lineage-replay">
      <header class="replay-head">
        <div>
          <h3>死信重放映射（共享）</h3>
          <p class="page-desc">与死信处理页读取同一份死信重放映射数据，按当前有效版本建立上游分区到下游转换节点的边。</p>
        </div>
        <div class="replay-check">
          <span>版本 <strong>{{ replayVersion ?? '—' }}</strong></span>
          <span>映射 <strong>{{ replayMappings.length }}</strong></span>
          <span>重放边 <strong>{{ replayEdges.length }}</strong></span>
          <span :class="replayCheck.primary_ok === false ? 'error-text' : ''">主重放唯一 <strong>{{ replayCheck.primary_ok === false ? '否' : '是' }}</strong></span>
          <span :class="replayCheck.atomic_ok === false ? 'error-text' : ''">映射边配对 <strong>{{ replayCheck.atomic_ok === false ? '否' : '是' }}</strong></span>
        </div>
      </header>
      <table class="data-table">
        <thead>
          <tr><th>映射编号</th><th>版本</th><th>死信编号</th><th>原始主题</th><th>原始分区</th><th>转换节点</th><th>下游表名</th><th>主重放</th><th>映射状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in replayMappings" :key="String(item.id)" :data-testid="`lineage-mapping-${item.id}`">
            <td>{{ item['映射编号'] }}</td>
            <td>{{ item['版本'] }}</td>
            <td>{{ item['死信编号'] }}</td>
            <td>{{ item['原始主题'] }}</td>
            <td>{{ item['原始分区'] }}</td>
            <td>{{ item['转换节点'] }}</td>
            <td>{{ item['下游表名'] }}</td>
            <td>{{ item['主重放'] ? '是' : '否' }}</td>
            <td>{{ item['映射状态'] }}</td>
          </tr>
          <tr v-if="!replayMappings.length"><td colspan="9" class="empty-state">暂无重放映射，可在死信处理页生成</td></tr>
        </tbody>
      </table>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记血缘记录</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot"><span>{{ graph.nodes.length }} 个节点</span></footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/stream_lineage'
const columns = ["记录编号", "上游主题", "下游表名", "转换节点", "字段映射", "采集时间", "延迟偏移", "链路版本"]
const actions = ["提交校验", "标记生效", "停用记录"]
const statuses = ["草稿", "待校验", "已生效", "已停用"]
const STATUS_COLORS: Record<string,string> = { '草稿':'#e2e8f0','待校验':'#fef3c7','已启用':'#d1fae5','已停用':'#fee2e2' }
const nodeColor = (s: string) => STATUS_COLORS[s] || '#e2e8f0'
const graph = reactive<{nodes: any[], edges: any[]}>({ nodes: [], edges: [] })
const nodePos = ref<Record<string,{x:number;y:number}>>({})
const selectedNode = ref<any>(null)
const detailFields = computed(() => columns.filter((f: string) => !['id','name','label','status'].includes(f)))
const svgW = ref(800); const svgH = ref(400)
const errorMessage = ref('')
const creating = ref(false); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const replayMappings = ref<any[]>([]); const replayVersion = ref<number|null>(null); const replayCheck = ref<Record<string, any>>({})
const replayEdges = computed(() => graph.edges.filter((edge: any) => edge.label === '死信重放'))
let dragNode: any = null; const dagRef = ref<HTMLElement|null>(null)
function layout(nodes: any[]) {
  const w = 800; const h = Math.max(400, Math.ceil(nodes.length / 5) * 80 + 60)
  svgW.value = w; svgH.value = h
  const pos: Record<string,{x:number;y:number}> = {}
  nodes.forEach((n,i) => { pos[String(n.id)] = { x: 120 + (i % 5) * 150, y: 50 + Math.floor(i / 5) * 80 } })
  nodePos.value = pos
}
function selectNode(node: any) { selectedNode.value = selectedNode.value?.id === node.id ? null : node }
function isLineageNode(node: any) { return node?.type === 'lineage' }
function startDrag(e: MouseEvent, node: any) { dragNode = node; const onMove = (ev: MouseEvent) => { if(!dragNode) return; nodePos.value[String(node.id)] = { x: ev.offsetX, y: ev.offsetY } }; const onUp = () => { dragNode = null; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp) }; document.addEventListener('mousemove', onMove); document.addEventListener('mouseup', onUp) }
async function loadReplay() {
  try {
    const res = await request(`${ENDPOINT}/replay-mappings`)
    const data = await res.json()
    replayMappings.value = data.items || []
    replayVersion.value = data.version ?? null
    replayCheck.value = data.check || {}
  } catch(e) { replayMappings.value = [] }
}
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); graph.nodes = data.nodes || []; graph.edges = data.edges || []; layout(graph.nodes); await loadReplay() } catch(e) { errorMessage.value = '加载失败' }
}
async function runAction(action: string, node: any) {
  try { await request(`${ENDPOINT}/${node.id}/actions`, { method: 'POST', body: JSON.stringify({ action }) }); await reload() } catch(e) { errorMessage.value = '操作失败' }
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(reload)
</script>

<style scoped>
.dag-edge-replay { stroke: #f59e0b; stroke-dasharray: 4 3; }
.replay-panel { margin-top: 16px; padding: 14px; background: #fff; border: 1px solid var(--border); border-radius: 8px; }
.replay-head { display: flex; justify-content: space-between; gap: 16px; flex-wrap: wrap; align-items: flex-start; }
.replay-head h3 { margin: 0; font-size: 15px; }
.replay-check { display: flex; flex-wrap: wrap; gap: 12px; font-size: 12px; color: var(--muted); }
.replay-check strong { color: #1f2937; }
</style>
