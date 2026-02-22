/* ============================================================
   Generations Study – Derived Data Dashboard  |  app.js
   Client-side only. No data leaves the browser.
   ============================================================ */

'use strict';

// ── Schema metadata ─────────────────────────────────────────────────────────
// Mutable: replaced when a schema file is loaded or data is auto-inferred.
// The block below is the built-in derived-data schema kept as the default.
let SCHEMA = {
  R0_TCode:               { desc:'Pseudo-anonymised 8-character study identifier', group:'id',            type:'string' },
  R0_Ethnicity:           { desc:'Ethnicity of the study participant',              group:'demographics',  type:'categorical',
    codes:{1:'White',2:'Black',3:'Asian',4:'Other',9:'Not known'} },
  R0_AshkenaziAncestry:   { desc:'Ashkenazi Jewish ancestry flag',                 group:'demographics',  type:'binary',
    codes:{0:'No',1:'Yes'} },
  R0_Height:              { desc:'Height in centimetres at study entry',            group:'anthropometry', type:'numeric', unit:'cm' },
  R0_Weight:              { desc:'Weight in kilograms at study entry',              group:'anthropometry', type:'numeric', unit:'kg' },
  R0_BMI:                 { desc:'BMI at study entry (999=pregnant at entry)',      group:'anthropometry', type:'numeric', unit:'kg/m²', sentinel:999 },
  R0_WaistCircum:         { desc:'Waist circumference in centimetres at entry',    group:'anthropometry', type:'numeric', unit:'cm' },
  R0_HipCircum:           { desc:'Hip circumference in centimetres at entry',      group:'anthropometry', type:'numeric', unit:'cm' },
  R0_WaistHipRatio:       { desc:'Waist-to-hip ratio at entry (999=pregnant)',     group:'anthropometry', type:'numeric', sentinel:999 },
  R0_Height20:            { desc:'Height in centimetres at age 20 (999=<20 at entry)',group:'anthropometry',type:'numeric',unit:'cm',sentinel:999},
  R0_Weight20:            { desc:'Weight in kilograms at age 20 (999=<20 at entry)',  group:'anthropometry',type:'numeric',unit:'kg', sentinel:999},
  R0_BMI20:               { desc:'BMI at age 20 (999=<20 or pregnant at 20)',      group:'anthropometry', type:'numeric', unit:'kg/m²', sentinel:999 },
  R0_PregAtEntry:         { desc:'Pregnant at study entry',                        group:'reproductive',  type:'binary',
    codes:{0:'No',1:'Yes'} },
  R0_PregAt20:            { desc:'Pregnant at age 20 (999=was <20 at entry)',      group:'reproductive',  type:'categorical',
    codes:{0:'No',1:'Yes',999:'NA (<20)'} },
  R0_AgeMenarche:         { desc:'Age at menarche (first period), whole years',    group:'reproductive',  type:'integer', unit:'years' },
  R0_Parous:              { desc:'Parity status at entry',                         group:'reproductive',  type:'categorical',
    codes:{'-1':'Never pregnant',0:'Nulliparous',1:'Parous',9:'Ever preg, parity unknown'} },
  R0_Parity:              { desc:'Number of live-birth pregnancies at entry',      group:'reproductive',  type:'integer' },
  R0_AgeBirthFirst:       { desc:'Age at first live birth (999=no live birth)',    group:'reproductive',  type:'numeric', unit:'years', sentinel:999 },
  R0_AgeBirthLast:        { desc:'Age at last live birth (999=no live birth)',     group:'reproductive',  type:'numeric', unit:'years', sentinel:999 },
  R0_BreastfeedingDuration:{ desc:'Total weeks breastfed across live births (9999=no live birth)', group:'reproductive', type:'numeric', unit:'weeks', sentinel:9999 },
  R0_Breastfed:           { desc:'Ever breastfed (999=no live birth)',             group:'reproductive',  type:'categorical',
    codes:{0:'No',1:'Yes',999:'NA (no live birth)'} },
  R0_Menopause:           { desc:'Menopausal status at baseline',                  group:'reproductive',  type:'categorical',
    codes:{1:'Postmenopausal',2:'Premenopausal',3:'Assumed postmeno',4:'Assumed premeno',9:'Never had periods'} },
  R0_AgeMenopause:        { desc:'Age at menopause (years)',                       group:'reproductive',  type:'integer', unit:'years' },
  R0_MenopauseReason:     { desc:'Reason periods stopped',                        group:'reproductive',  type:'categorical',
    codes:{1:'Natural',2:'Bilateral oophorectomy',3:'Hysterectomy only',4:'Surgery (type unknown)',
           5:'Chemo/radio/cancer tx',6:'Unknown reason',7:'Other reason',8:'On hormones',9:'On HRT',
           10:'Stress',11:'Breastfeeding/pregnant',12:'Perimenopausal',13:'Natural on HRT/OC',
           14:'Eating disorder',15:'Illness',16:'Premenopausal',17:'Status unknown',
           18:'Other surgery',19:'Never had periods'} },
  R0_OralContraceptiveStatus:{ desc:'Oral contraceptive use status at entry',     group:'lifestyle',     type:'categorical',
    codes:{0:'Never',1:'Former',2:'Current'} },
  R0_AgeStartedOC:        { desc:'Age first used oral contraceptives (999=never)',  group:'lifestyle',     type:'numeric', unit:'years', sentinel:999 },
  R0_AgeLastUsedOC:       { desc:'Age last used OC (999=current user)',            group:'lifestyle',     type:'numeric', unit:'years', sentinel:999 },
  R0_OCLength:            { desc:'Total duration of OC use (years)',               group:'lifestyle',     type:'numeric', unit:'years' },
  R0_HRTStatus:           { desc:'Menopausal hormone treatment status',            group:'lifestyle',     type:'categorical',
    codes:{0:'Never',1:'Former',2:'Current'} },
  R0_HRTStartAge:         { desc:'Age started HRT',                               group:'lifestyle',     type:'integer', unit:'years' },
  R0_HRTStopAge:          { desc:'Age stopped HRT',                               group:'lifestyle',     type:'integer', unit:'years' },
  R0_HRTDuration:         { desc:'Total duration of HRT use (years)',              group:'lifestyle',     type:'numeric', unit:'years' },
  R0_AlcoholStatus:       { desc:'Alcohol use status at baseline',                group:'lifestyle',     type:'categorical',
    codes:{0:'Never',1:'Former',2:'Current'} },
  R0_AgeStartedDrinking:  { desc:'Age started regularly drinking alcohol',        group:'lifestyle',     type:'integer', unit:'years' },
  R0_AgeStoppedDrinking:  { desc:'Age stopped regularly drinking alcohol',        group:'lifestyle',     type:'integer', unit:'years' },
  R0_AlcoholUnitsPerWeek: { desc:'Weekly alcohol units (current drinkers)',       group:'lifestyle',     type:'numeric', unit:'units/wk' },
  R0_SmokingStatus:       { desc:'Cigarette smoking status at baseline',          group:'lifestyle',     type:'categorical',
    codes:{0:'Never',1:'Former',2:'Current'} },
  R0_AgeStartedSmoking:   { desc:'Age started cigarette smoking',                group:'lifestyle',     type:'integer', unit:'years' },
  R0_AgeStoppedSmoking:   { desc:'Age stopped cigarette smoking',                group:'lifestyle',     type:'integer', unit:'years' },
  R0_CigsPerDay:          { desc:'Cigarettes smoked per day (current smokers)',   group:'lifestyle',     type:'numeric', unit:'cigs/day' },
  R0_PackYears:           { desc:'Cumulative smoking exposure (pack-years)',      group:'lifestyle',     type:'numeric', unit:'pack-yrs' },
  R0_PhysicalActivity:    { desc:'Physical activity (MET-hours/week)',            group:'lifestyle',     type:'numeric', unit:'MET-h/wk' },
  R0_GreenVegDailyServings:{ desc:'Average daily green vegetable servings',      group:'lifestyle',     type:'integer', unit:'servings/day' },
  R0_FruitDailyServings:  { desc:'Average daily fruit servings',                 group:'lifestyle',     type:'integer', unit:'servings/day' },
  R0_BBD:                 { desc:'History of benign breast disease',              group:'medical',       type:'binary',
    codes:{0:'No',1:'Yes'} },
  R0_DiabetesStatus:      { desc:'Diabetes diagnosis at baseline',               group:'medical',       type:'binary',
    codes:{0:'No',1:'Yes'} },
  R0_AgeDiabetes:         { desc:'Age at diabetes diagnosis (years)',             group:'medical',       type:'integer', unit:'years' },
  R0_DiabetesInsulin:     { desc:'Diabetes treated with insulin at baseline',    group:'medical',       type:'binary',
    codes:{0:'No',1:'Yes'} },
  R0_FamHistCancer:       { desc:'Family history of any cancer (1st degree)',    group:'family',        type:'binary',
    codes:{0:'No',1:'Yes'} },
  R0_FamHistCancerNum:    { desc:'No. of 1st-degree relatives with any cancer',  group:'family',        type:'integer' },
  R0_FamHistBC:           { desc:'Family history of breast cancer (1st degree)', group:'family',        type:'binary',
    codes:{0:'No',1:'Yes'} },
  R0_FamHistBCNum:        { desc:'No. of 1st-degree relatives with breast cancer',group:'family',       type:'integer' },
  R0_FamHistOV:           { desc:'Family history of ovarian cancer (1st degree)',group:'family',        type:'binary',
    codes:{0:'No',1:'Yes'} },
  R0_FamHistOVNum:        { desc:'No. of 1st-degree relatives with ovarian cancer',group:'family',      type:'integer' },
  R0_FamHistColo:         { desc:'Family history of colorectal cancer (1st degree)',group:'family',     type:'binary',
    codes:{0:'No',1:'Yes'} },
  R0_FamHistColoNum:      { desc:'No. of 1st-degree relatives with colorectal cancer',group:'family',   type:'integer' },
  R0_FamHistProst:        { desc:'Family history of prostate cancer (1st degree)',group:'family',       type:'binary',
    codes:{0:'No',1:'Yes'} },
  R0_FamHistProstNum:     { desc:'No. of 1st-degree relatives with prostate cancer',group:'family',     type:'integer' },
};

const BUILTIN_SCHEMA       = SCHEMA;  // preserved reference to built-in
const BUILTIN_GROUP_LABELS = {
  id:'Identifier', demographics:'Demographics', anthropometry:'Anthropometry',
  reproductive:'Reproductive', lifestyle:'Lifestyle', medical:'Medical', family:'Family History'
};
let GROUP_LABELS = Object.assign({}, BUILTIN_GROUP_LABELS);
let schemaSource = 'builtin'; // 'builtin' | 'file' | 'inferred'

// ── App state ──────────────────────────────────────────────────────────────
let rawData      = [];
let cohortData   = [];
let filters      = [];          // [{field, operator, value, logic}]
let activeVar    = null;
let mainChart    = null;
let missChart    = null;
let chartMode    = 'histogram'; // 'histogram' | 'violin' | 'bar'
let activeGroup  = 'all';

// ── Utilities ──────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const qs = sel => document.querySelector(sel);

function fmt(n, dec=1) {
  if (n === null || n === undefined || isNaN(n)) return '—';
  return Number(n).toFixed(dec);
}
function pct(n, total) { return total ? ((n / total) * 100).toFixed(1) + '%' : '—'; }

function getValidValues(key, data) {
  const s = SCHEMA[key];
  const sentinel = s ? s.sentinel : null;
  return data
    .map(r => r[key])
    .filter(v => v !== null && v !== undefined && v !== sentinel
             && !(sentinel === 999 && v === 9999));
}

function isMissing(v, sentinel) {
  if (v === null || v === undefined) return true;
  if (sentinel && (v === sentinel || v === 9999)) return false; // sentinel ≠ missing
  return false;
}

function summarise(values) {
  const nums = values.map(Number).filter(n => !isNaN(n));
  if (!nums.length) return null;
  nums.sort((a, b) => a - b);
  const n = nums.length;
  const mean = nums.reduce((s, v) => s + v, 0) / n;
  const variance = nums.reduce((s, v) => s + (v - mean) ** 2, 0) / n;
  const q = p => {
    const idx = p * (n - 1);
    const lo = Math.floor(idx), hi = Math.ceil(idx);
    return nums[lo] + (nums[hi] - nums[lo]) * (idx - lo);
  };
  return { n, mean, sd: Math.sqrt(variance),
    min: nums[0], q1: q(0.25), median: q(0.5), q3: q(0.75), max: nums[n-1] };
}

function freqTable(values) {
  const counts = {};
  values.forEach(v => { const k = String(v); counts[k] = (counts[k] || 0) + 1; });
  return Object.entries(counts).sort((a, b) => b[1] - a[1]);
}

function toast(msg, ms=2500) {
  const el = $('toast');
  el.textContent = msg;
  el.style.display = 'block';
  setTimeout(() => { el.style.display = 'none'; }, ms);
}

function typeClass(t) {
  return `type-${t}`;
}

// ── File loading ───────────────────────────────────────────────────────────
// ── Schema parsing ──────────────────────────────────────────────────────────

// Convert a loaded JSON object into {schema, groupLabels}.
// Supports:
//   1. JSON Schema Draft 2020-12  (has "properties" or "$schema")
//   2. App internal format        (object of {desc, type, group, ...} entries)
function parseSchemaFile(json) {
  let schema = {}, groupLabels = {};

  // Helper: resolve JSON Schema "type" which may be a string OR array
  // e.g. "number" | ["number","null"] | ["null","integer"] → "number"
  function resolveJsonType(t) {
    if (!t) return null;
    if (Array.isArray(t)) return t.find(x => x !== 'null') || null;
    return t;
  }

  // Helper: given a full property object, find the underlying non-null raw type.
  // Handles plain "type", ["number","null"] arrays, and oneOf/anyOf nullable patterns.
  function inferRawType(prop) {
    const t = resolveJsonType(prop.type);
    if (t) return t;
    // oneOf/anyOf: look for a non-null, non-const type variant (the real data type)
    const variants = prop.oneOf || prop.anyOf;
    if (Array.isArray(variants)) {
      for (const v of variants) {
        if (v.type && v.type !== 'null' && v.const === undefined) return v.type;
      }
    }
    return null;
  }

  // Helper: extract code map from several possible JSON Schema patterns
  function extractCodes(prop) {
    // 1. explicit x-codes / x-labels: {"0":"No","1":"Yes"}
    if (prop['x-codes'])  return prop['x-codes'];
    if (prop['x-labels']) return prop['x-labels'];
    if (prop.codes)       return prop.codes;
    // 2. oneOf / anyOf array: [{const:0,title:"No"},{const:1,title:"Yes"}]
    const variants = prop.oneOf || prop.anyOf;
    if (Array.isArray(variants)) {
      // If any variant has a numeric/integer type (without const), this is a
      // numeric-with-sentinel pattern — const variants are sentinels, not codes.
      // Don't extract codes; sentinel will be captured separately.
      const hasNumericType = variants.some(
        v => (v.type === 'number' || v.type === 'integer') && v.const === undefined
      );
      if (hasNumericType) return null;
      const map = {};
      variants.forEach(v => {
        if (v.const !== undefined && v.title) map[String(v.const)] = v.title;
        else if (v.const !== undefined && v.description) map[String(v.const)] = v.description;
      });
      if (Object.keys(map).length) return map;
    }
    // 3. enum + enumNames arrays: enum:[0,1,2], enumNames:["No","Yes","Unknown"]
    if (Array.isArray(prop.enum) && Array.isArray(prop.enumNames)) {
      const map = {};
      prop.enum.forEach((v, i) => {
        if (v !== null) map[String(v)] = prop.enumNames[i] || String(v);
      });
      return map;
    }
    // 3b. enum + enumDescriptions: ["0: Never", "1: Former", "2: Current", "null: Missing"]
    //     Parse "VALUE: Label text." format — skip null entries
    if (Array.isArray(prop.enum) && Array.isArray(prop.enumDescriptions)) {
      const map = {};
      prop.enumDescriptions.forEach(desc => {
        const colonIdx = desc.indexOf(': ');
        if (colonIdx === -1) return;
        const valStr = desc.substring(0, colonIdx).trim();
        if (valStr.toLowerCase() === 'null') return;  // skip null entries
        const label = desc.substring(colonIdx + 2).replace(/\.$/, '').trim();
        map[valStr] = label;
      });
      if (Object.keys(map).length) return map;
    }
    // 4. enum alone (no labels): build code map with value as label, skip null
    if (Array.isArray(prop.enum) && prop.enum.length <= 20) {
      const map = {};
      prop.enum.forEach(v => { if (v !== null) map[String(v)] = String(v); });
      return map;
    }
    return null;
  }

  // ── Log a sample to help debug unknown schema formats ───────────────────
  if (json.properties || json.$schema) {
    const sampleKeys = Object.keys(json.properties || {}).slice(0, 3);
    console.log('[schema parser] JSON Schema detected. Sample properties:', sampleKeys.map(k => ({
      key: k, type: (json.properties||{})[k]?.type,
      keys: Object.keys((json.properties||{})[k] || {})
    })));
  }

  if (json.properties || json.$schema) {
    // ── JSON Schema Draft 2020-12 (or similar) ──────────────────────────
    const props = json.properties || {};
    const groups = new Set();

    Object.entries(props).forEach(([key, prop]) => {
      const rawJsonType = inferRawType(prop);   // handles plain type, array, and oneOf patterns
      const xType    = prop['x-type'] || prop['x-variableType'] || prop.variableType || null;
      const codes    = extractCodes(prop);

      // Sentinel: explicit field first, then look inside oneOf for a const:'NA' variant
      let sentinel = prop['x-sentinel'] !== undefined ? prop['x-sentinel']
                   : prop.sentinel      !== undefined ? prop.sentinel : null;
      if (sentinel === null) {
        const variants = prop.oneOf || prop.anyOf;
        if (Array.isArray(variants)) {
          for (const v of variants) {
            if (v.const !== undefined && (v.title === 'NA' || v.description === 'NA'
                || String(v.title).toLowerCase().includes('not applicable')
                || String(v.description).toLowerCase().includes('not applicable'))) {
              sentinel = v.const;
              break;
            }
          }
        }
      }

      const group    = prop['x-group'] || prop['x-category'] || prop.group || prop.category || 'data';
      const desc     = prop.description || prop.title || key;
      // Unit: explicit field first, then extract from description parenthetical
      // e.g. "Physical activity (hours/week)." → "hours/week"
      // Reject multi-word qualifiers like "(current smokers)" by requiring no spaces
      let unit = prop['x-unit'] || prop.unit || null;
      if (!unit) {
        const m = desc.match(/\(([^)]+)\)\s*\.?\s*$/);
        if (m && !/\s/.test(m[1])) unit = m[1];
      }

      // Determine internal type in priority order:
      // 1. Explicit x-type override
      // 2. Has a code map → binary (≤2 non-sentinel codes) or categorical
      // 3. JSON Schema primitive type (resolved via inferRawType)
      // 4. Fallback to 'numeric' (safer than 'string' for unknown numeric fields)
      let type = xType;
      if (!type) {
        if (codes) {
          const nonSentinelKeys = Object.keys(codes).filter(k => +k !== 999 && +k !== 9999);
          type = nonSentinelKeys.length <= 2 ? 'binary' : 'categorical';
        } else if (rawJsonType === 'number')  { type = 'numeric';  }
        else if (rawJsonType === 'integer')   { type = 'integer';  }
        else if (rawJsonType === 'boolean')   { type = 'binary';   }
        else if (rawJsonType === 'string')    { type = 'string';   }
        else                                  { type = 'numeric';  } // safer default
      }

      const entry = { desc, group, type };
      if (unit)            entry.unit     = unit;
      if (sentinel !== null) entry.sentinel = sentinel;
      if (codes)           entry.codes    = codes;
      groups.add(group);
      schema[key] = entry;
    });

    groups.forEach(g => {
      // Use top-level x-groupLabels map if present, otherwise capitalise
      const labelMap = json['x-groupLabels'] || json.groupLabels || {};
      groupLabels[g] = labelMap[g] || (g.charAt(0).toUpperCase() + g.slice(1).replace(/[_-]/g, ' '));
    });

  } else {
    // ── App internal format ──────────────────────────────────────────────
    // Top-level keys are variable names mapping to {desc, type, group, ...}
    schema = json;
    const groups = new Set(Object.values(json).map(v => v.group).filter(Boolean));
    groups.forEach(g => {
      groupLabels[g] = g.charAt(0).toUpperCase() + g.slice(1).replace(/[_-]/g, ' ');
    });
  }

  // If the schema has no group information, backfill from BUILTIN_SCHEMA
  // so group filter buttons still work for known derived variables.
  const allDefault = Object.values(schema).every(v => !v.group || v.group === 'data');
  if (allDefault) {
    Object.keys(schema).forEach(key => {
      if (BUILTIN_SCHEMA[key] && BUILTIN_SCHEMA[key].group) {
        schema[key].group = BUILTIN_SCHEMA[key].group;
      }
    });
    // Add built-in group labels for every group now referenced
    const usedGrps = new Set(Object.values(schema).map(v => v.group).filter(Boolean));
    usedGrps.forEach(g => {
      if (!groupLabels[g]) groupLabels[g] = BUILTIN_GROUP_LABELS[g] || (g.charAt(0).toUpperCase() + g.slice(1).replace(/[_-]/g, ' '));
    });
  }

  console.log(`[schema parser] Parsed ${Object.keys(schema).length} variables. Types:`,
    Object.values(schema).reduce((acc, v) => { acc[v.type] = (acc[v.type]||0)+1; return acc; }, {}));

  return { schema, groupLabels };
}

// Auto-infer a minimal schema from data when no schema file is provided.
function inferSchemaFromData(data) {
  if (!data.length) return { schema: {}, groupLabels: { data: 'Data' } };
  const keys = Object.keys(data[0]);
  const schema = {};
  keys.forEach(key => {
    const vals = data.map(r => r[key]).filter(v => v !== null && v !== undefined);
    if (!vals.length) { schema[key] = { desc: key, group: 'data', type: 'string' }; return; }
    const nums = vals.map(Number).filter(n => !isNaN(n));
    const unique = [...new Set(vals.map(String))].sort((a, b) => +a - +b);
    let type, codes;
    if (nums.length === vals.length) {
      if (unique.length <= 2)  { type = 'binary';      codes = Object.fromEntries(unique.map(v => [v, v])); }
      else if (unique.length <= 12) { type = 'categorical'; codes = Object.fromEntries(unique.map(v => [v, v])); }
      else if (nums.every(n => Number.isInteger(n))) { type = 'integer'; }
      else { type = 'numeric'; }
    } else {
      type = 'string';
    }
    const entry = { desc: key, group: 'data', type };
    if (codes) entry.codes = codes;
    schema[key] = entry;
  });
  return { schema, groupLabels: { data: 'Data' } };
}

// ── File loading ────────────────────────────────────────────────────────────

let pendingData   = null; // data loaded but schema not yet resolved
let pendingSchema = null; // parsed schema waiting for data
let dataFileName  = '';   // filename of the loaded data JSON
let schemaFileName = '';  // filename of the loaded schema JSON

function setupDrop() {
  // Data panel
  const dzData = $('dz-data');
  const fiData = $('file-input');
  dzData.addEventListener('click', () => fiData.click());
  fiData.addEventListener('change', e => { if (e.target.files[0]) loadDataFile(e.target.files[0]); });
  dzData.addEventListener('dragover', e => { e.preventDefault(); dzData.classList.add('dragover'); });
  dzData.addEventListener('dragleave', () => dzData.classList.remove('dragover'));
  dzData.addEventListener('drop', e => {
    e.preventDefault(); dzData.classList.remove('dragover');
    if (e.dataTransfer.files[0]) loadDataFile(e.dataTransfer.files[0]);
  });

  // Schema panel
  const dzSchema = $('dz-schema');
  const fiSchema = $('schema-input');
  dzSchema.addEventListener('click', () => fiSchema.click());
  fiSchema.addEventListener('change', e => { if (e.target.files[0]) loadSchemaFile(e.target.files[0]); });
  dzSchema.addEventListener('dragover', e => { e.preventDefault(); dzSchema.classList.add('dragover'); });
  dzSchema.addEventListener('dragleave', () => dzSchema.classList.remove('dragover'));
  dzSchema.addEventListener('drop', e => {
    e.preventDefault(); dzSchema.classList.remove('dragover');
    if (e.dataTransfer.files[0]) loadSchemaFile(e.dataTransfer.files[0]);
  });
}

function loadDataFile(file) {
  const reader = new FileReader();
  reader.onload = e => {
    try {
      const data = JSON.parse(e.target.result);
      if (!Array.isArray(data)) throw new Error('Expected a JSON array of records');
      pendingData  = data;
      dataFileName = file.name;
      // Update data panel UI
      $('dz-data-hint').style.display = 'none';
      $('btn-data-select').style.display = 'none';
      $('dz-data-ok').style.display = '';
      $('dz-data-name').textContent = `${file.name}  ·  ${data.length.toLocaleString()} records`;
      $('dz-data').classList.add('loaded');
      // Show Continue button — user can still add schema before proceeding
      $('dz-continue').style.display = '';
    } catch (err) { alert('Failed to parse data file: ' + err.message); }
  };
  reader.readAsText(file);
}

function continueToApp() {
  if (!pendingData) return;
  if (pendingSchema) {
    applySchema(pendingSchema.schema, pendingSchema.groupLabels, 'file');
  } else {
    const inferred = inferSchemaFromData(pendingData);
    applySchema(inferred.schema, inferred.groupLabels, 'inferred');
  }
  initApp(pendingData);
}

function loadSchemaFile(file, fromDashboard = false) {
  const reader = new FileReader();
  reader.onload = e => {
    try {
      const json = JSON.parse(e.target.result);
      const { schema, groupLabels } = parseSchemaFile(json);
      pendingSchema  = { schema, groupLabels };
      schemaFileName = file.name;

      // Update load-screen schema panel UI (may not be visible, but keep in sync)
      $('dz-schema-hint').style.display    = 'none';
      $('dz-schema-auto').style.display    = 'none';
      $('btn-schema-select').style.display = 'none';
      $('dz-schema-ok').style.display      = '';
      $('dz-schema-name').textContent      = `${file.name}  ·  ${Object.keys(schema).length} variables`;
      $('dz-schema').classList.add('loaded');

      applySchema(schema, groupLabels, 'file');

      // If already in the dashboard, re-render immediately with new schema
      if (fromDashboard && pendingData) {
        initApp(pendingData);
        toast(`✓ Schema updated: ${file.name}`);
      }
      // If still on load screen with data ready, show Continue if not shown
      if (!fromDashboard && pendingData && $('dz-continue').style.display === 'none') {
        $('dz-continue').style.display = '';
      }
    } catch (err) { alert('Failed to parse schema file: ' + err.message); }
  };
  reader.readAsText(file);
}

function reloadSchema() {
  const fi = $('schema-input-2');
  fi.onchange = e => { if (e.target.files[0]) loadSchemaFile(e.target.files[0], true); fi.value = ''; };
  fi.click();
}

function applySchema(schema, groupLabels_, source) {
  SCHEMA       = schema;
  GROUP_LABELS = groupLabels_;
  schemaSource = source;
  // Show auto-detected badge if no schema file
  if (source === 'inferred') {
    $('dz-schema-hint').style.display = 'none';
    $('dz-schema-auto').style.display = '';
  }
}

// ── Initialise app after data load ─────────────────────────────────────────
function initApp(data) {
  rawData    = data;
  cohortData = data;

  $('load-screen').classList.remove('active');
  $('sidebar').style.display   = 'flex';
  $('nav-tabs').style.display  = 'flex';
  $('cohort-badge').style.display = 'flex';

  $('total-count').textContent  = data.length;
  $('cohort-count').textContent = data.length;

  // Rebuild group filter buttons from current GROUP_LABELS
  activeGroup = 'all';
  const gf = $('group-filter');
  gf.innerHTML = '<button class="gf-btn active" data-group="all">All</button>';
  // Collect groups that actually appear in the schema
  const usedGroups = [...new Set(
    Object.values(SCHEMA).map(s => s.group).filter(g => g && g !== 'id')
  )];
  usedGroups.forEach(g => {
    const lbl = GROUP_LABELS[g] || (g.charAt(0).toUpperCase() + g.slice(1));
    const btn = document.createElement('button');
    btn.className = 'gf-btn';
    btn.dataset.group = g;
    btn.textContent = lbl;
    gf.appendChild(btn);
  });

  // ── Schema ↔ data mismatch check ────────────────────────────────────────
  if (data.length && schemaSource !== 'inferred') {
    const dataCols   = new Set(Object.keys(data[0]));
    const schemaCols = new Set(Object.keys(SCHEMA));
    const inDataOnly   = [...dataCols].filter(k => !schemaCols.has(k));
    const inSchemaOnly = [...schemaCols].filter(k => !dataCols.has(k));

    // Auto-add columns that are in data but missing from schema
    if (inDataOnly.length) {
      const extra = inferSchemaFromData(data);
      inDataOnly.forEach(k => { if (extra.schema[k]) SCHEMA[k] = extra.schema[k]; });
    }
    // Warn about schema variables absent from data
    if (inSchemaOnly.length) {
      setTimeout(() => toast(
        `⚠ ${inSchemaOnly.length} schema variable(s) not found in data (e.g. ${inSchemaOnly.slice(0,3).join(', ')})`
      , 5000), 800);
    }
    if (inDataOnly.length) {
      setTimeout(() => toast(
        `ℹ ${inDataOnly.length} data column(s) not in schema — types auto-inferred`
      , 4000), 400);
    }
  }

  renderVarList();
  populateStratSelects();
  showTab('overview');
  renderOverview();
  renderMissingness();
  buildAttritionFlow();

  const nVars = Object.keys(SCHEMA).filter(k => SCHEMA[k].type !== 'string').length;
  toast(`✓ Loaded ${data.length.toLocaleString()} records · ${nVars} variables · schema: ${schemaSource}`);
}

// ── Sidebar variable list ─────────────────────────────────────────────────
function renderVarList() {
  const search = $('var-search').value.toLowerCase();
  const list   = $('var-list');
  list.innerHTML = '';
  let shown = 0;

  const keys = Object.keys(SCHEMA).filter(k => k !== 'R0_TCode');

  keys.forEach(key => {
    const s = SCHEMA[key];
    if (activeGroup !== 'all' && s.group !== activeGroup) return;
    if (search && !key.toLowerCase().includes(search) && !s.desc.toLowerCase().includes(search)) return;

    shown++;
    const item = document.createElement('div');
    item.className = 'var-item' + (key === activeVar ? ' active' : '');
    item.innerHTML = `
      <div style="flex:1;overflow:hidden">
        <div class="var-name">${key}</div>
        <div class="var-desc">${s.desc}</div>
      </div>
      <span class="var-type-pill ${typeClass(s.type)}">${s.type}</span>`;
    item.addEventListener('click', () => selectVar(key));
    list.appendChild(item);
  });

  $('var-count-badge').textContent = `(${shown})`;
}

function selectVar(key) {
  activeVar = key;
  renderVarList();
  showTab('explore');
  renderVarDetail(key, cohortData);
}

// ── Tab navigation ─────────────────────────────────────────────────────────
function showTab(name) {
  document.querySelectorAll('.nav-tab').forEach(t => {
    t.classList.toggle('active', t.dataset.tab === name);
  });
  document.querySelectorAll('.tab-panel').forEach(p => {
    p.classList.remove('active');
  });
  const panel = $('tab-' + name) || $('load-screen');
  if (panel) panel.classList.add('active');
}

// ── Overview tab ───────────────────────────────────────────────────────────
function renderOverview() {
  const n = rawData.length;
  const keys = Object.keys(SCHEMA).filter(k => k !== 'R0_TCode');
  const totalCells = n * keys.length;
  const missingCells = keys.reduce((acc, k) => {
    return acc + rawData.filter(r => r[k] === null || r[k] === undefined).length;
  }, 0);

  $('overview-stats').innerHTML = `
    <div class="stat-card"><div class="stat-value">${n.toLocaleString()}</div><div class="stat-label">Participants</div></div>
    <div class="stat-card"><div class="stat-value">${keys.length}</div><div class="stat-label">Variables</div></div>
    <div class="stat-card"><div class="stat-value">${((1 - missingCells / totalCells) * 100).toFixed(1)}%</div><div class="stat-label">Completeness</div></div>
    <div class="stat-card"><div class="stat-value">${(missingCells / totalCells * 100).toFixed(1)}%</div><div class="stat-label">Overall Missing</div></div>
  `;

  const groups = {};
  keys.forEach(k => {
    const g = SCHEMA[k].group;
    groups[g] = (groups[g] || []);
    groups[g].push(k);
  });

  $('group-summary').innerHTML = Object.entries(groups).map(([g, ks]) => `
    <div style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid var(--border)">
      <span style="font-weight:700;color:var(--navy);min-width:160px">${GROUP_LABELS[g] || g}</span>
      <span style="color:var(--muted);font-size:12px">${ks.length} variables</span>
      <span style="font-size:11px;color:var(--muted)">${ks.slice(0,5).join(', ')}${ks.length > 5 ? '…' : ''}</span>
    </div>`).join('');
}

// ── Explore: variable detail ───────────────────────────────────────────────
function renderVarDetail(key, data) {
  const s = SCHEMA[key];
  if (!s) return;

  $('explore-prompt').style.display = 'none';
  $('var-detail').style.display     = 'block';
  $('detail-title').innerHTML = `${key} <span class="badge">${s.group}</span> <span style="font-size:12px;font-weight:400;color:var(--muted)">${s.unit || ''}</span>`;

  // Meta
  $('detail-meta').innerHTML = `
    <div class="var-meta-row"><span class="meta-label">Description</span><span class="meta-val">${s.desc}</span></div>
    <div class="var-meta-row"><span class="meta-label">Type</span><span class="meta-val"><span class="var-type-pill ${typeClass(s.type)}">${s.type}</span></span></div>
    ${s.sentinel !== undefined ? `<div class="var-meta-row"><span class="meta-label">Sentinel</span><span class="meta-val">${s.sentinel} = Not Applicable</span></div>` : ''}
  `;

  const allVals  = data.map(r => r[key]);
  const sentinel = s.sentinel;
  const validVals= getValidValues(key, data);
  const nullCount= allVals.filter(v => v === null || v === undefined).length;
  const sentCount= sentinel ? allVals.filter(v => v === sentinel || v === 9999).length : 0;

  // Summary stats
  const statsEl = $('detail-stats');
  statsEl.innerHTML = '';

  function statChip(val, label) {
    const d = document.createElement('div');
    d.className = 'stats-mini-item';
    d.innerHTML = `<div class="sv">${val}</div><div class="sl">${label}</div>`;
    statsEl.appendChild(d);
  }

  statChip(validVals.length, 'Valid');
  statChip(`${pct(nullCount, data.length)}`, 'Missing (null)');
  if (sentCount > 0) statChip(sentCount, `Sentinel (${sentinel})`);

  if (s.type === 'numeric' || s.type === 'integer') {
    const ss = summarise(validVals);
    if (ss) {
      statChip(fmt(ss.mean, 1), 'Mean');
      statChip(fmt(ss.sd, 1), 'SD');
      statChip(fmt(ss.median, 1), 'Median');
      statChip(`${fmt(ss.min,1)} – ${fmt(ss.max,1)}`, 'Range');
    }
  } else {
    const ft = freqTable(validVals);
    const top = ft.slice(0, 3);
    top.forEach(([code, cnt]) => {
      const label = (s.codes && s.codes[code]) ? s.codes[code] : code;
      statChip(cnt, label.length > 12 ? label.slice(0,12)+'…' : label);
    });
  }

  // Chart type buttons
  const btnContainer = $('chart-type-btns');
  btnContainer.innerHTML = '';
  const isNumeric = s.type === 'numeric' || s.type === 'integer';
  const modes = isNumeric ? ['histogram', 'violin', 'bar'] : ['bar', 'pie'];
  chartMode = modes[0]; // reset before buttons are drawn so active class is correct
  modes.forEach(mode => {
    const b = document.createElement('button');
    b.className = 'chart-btn' + (chartMode === mode ? ' active' : '');
    b.textContent = {histogram:'Histogram', violin:'Violin Plot', bar:'Bar chart', pie:'Pie chart'}[mode];
    b.onclick = () => {
      chartMode = mode;
      btnContainer.querySelectorAll('.chart-btn').forEach(x => x.classList.remove('active'));
      b.classList.add('active');
      drawChart(key, data);
    };
    btnContainer.appendChild(b);
  });

  // Download PNG button — added after loop so innerHTML='' doesn't remove it
  const dlBtn = document.createElement('button');
  dlBtn.className = 'btn-download-png';
  dlBtn.title = 'Download chart as PNG';
  dlBtn.innerHTML = '&#8595; PNG';
  dlBtn.onclick = () => App.downloadChartPNG();
  btnContainer.appendChild(dlBtn);

  drawChart(key, data);

  // Enum legend
  const enumCard = $('enum-card');
  if (s.codes) {
    enumCard.style.display = 'block';
    $('enum-legend').innerHTML = Object.entries(s.codes)
      .map(([code, label]) => `<div class="enum-item"><span class="enum-code">${code}</span><span>${label}</span></div>`)
      .join('');
  } else {
    enumCard.style.display = 'none';
  }
}

// ── Chart drawing ──────────────────────────────────────────────────────────
function drawChart(key, data) {
  if (mainChart) { mainChart.destroy(); mainChart = null; }
  const ctx = $('main-chart').getContext('2d');
  const s   = SCHEMA[key];
  const values = getValidValues(key, data);

  const PALETTE = ['#6B58A0','#B9E05D','#A599C7','#d46c00','#3a3358','#3d8b5c','#c0392b','#4f3f7a'];

  if (chartMode === 'histogram') {
    const nums = values.map(Number).filter(n => !isNaN(n));
    const min = Math.min(...nums), max = Math.max(...nums);
    const range = max - min;
    const isInt = s.type === 'integer';

    let buckets, labels;

    if (isInt && range <= 50) {
      // One bar per integer value — no decimals, no gaps
      const numBins = range + 1;
      labels  = Array.from({length: numBins}, (_, i) => String(Math.round(min) + i));
      buckets = Array(numBins).fill(0);
      nums.forEach(v => {
        const idx = Math.round(v) - Math.round(min);
        if (idx >= 0 && idx < numBins) buckets[idx]++;
      });
    } else {
      // Continuous or wide-range integer: aligned bins
      const TARGET_BINS = 30;
      const step = isInt
        ? Math.max(1, Math.ceil(range / TARGET_BINS))   // integer step ≥ 1
        : (range / TARGET_BINS || 1);
      const numBins = isInt
        ? Math.ceil((range + 1) / step)
        : TARGET_BINS;
      labels = Array.from({length: numBins}, (_, i) => {
        const lo = min + i * step;
        if (isInt) {
          const hi = Math.min(lo + step - 1, max);
          return step === 1 ? String(Math.round(lo)) : `${Math.round(lo)}–${Math.round(hi)}`;
        }
        return fmt(lo, 1);
      });
      buckets = Array(numBins).fill(0);
      nums.forEach(v => {
        const idx = Math.min(numBins - 1, Math.floor((v - min) / step));
        buckets[idx]++;
      });
    }

    mainChart = new Chart(ctx, {
      type: 'bar',
      data: { labels, datasets: [{ label: key, data: buckets,
        backgroundColor: '#6B58A088', borderColor: '#6B58A0', borderWidth: 1 }] },
      options: { maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: {
        x: { title: { display: true, text: s.unit ? `${key} (${s.unit})` : key } },
        y: { title: { display: true, text: 'Count' } }
      }}
    });

  } else if (chartMode === 'violin') {
    const nums = values.map(Number).filter(n => !isNaN(n));
    if (!nums.length) return;
    mainChart = new Chart(ctx, {
      type: 'violin',
      data: {
        labels: [s.label || key],
        datasets: [{
          label: key,
          data: [nums],
          backgroundColor: '#6B58A030',
          borderColor: '#6B58A0',
          borderWidth: 2,
          medianColor: '#B9E05D',
          meanColor: '#B9E05D',
          itemRadius: 0,
          barPercentage: 0.25,
          categoryPercentage: 1.0,
        }]
      },
      options: {
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          y: {
            title: { display: true, text: s.unit ? `${key} (${s.unit})` : key },
            min: Math.min(...nums) - (Math.max(...nums) - Math.min(...nums)) * 0.05,
            max: Math.max(...nums) + (Math.max(...nums) - Math.min(...nums)) * 0.05,
          }
        }
      }
    });

  } else {
    // Bar chart or Pie chart (categorical / integer counts)
    let ft;
    if (s.type === 'integer' && new Set(values).size > 15) {
      // bin if too many unique int values
      const nums = values.map(Number);
      const min = Math.min(...nums), max = Math.max(...nums);
      const bins = 12, step = Math.ceil((max - min + 1) / bins);
      const buckets = {}, labels = [];
      for (let i = min; i <= max; i += step) {
        const label = i + (step > 1 ? `–${i + step - 1}` : '');
        labels.push(label);
        buckets[label] = 0;
      }
      nums.forEach(v => {
        const idx = Math.floor((v - min) / step);
        const label = labels[Math.min(idx, labels.length - 1)];
        if (label !== undefined) buckets[label]++;
      });
      ft = Object.entries(buckets);
    } else {
      ft = freqTable(values.map(String));
      ft = ft.slice(0, 20); // cap for readability
    }

    // Sort order:
    //  - Numeric/integer type: numeric ascending, sentinels last
    //  - Binary/categorical with yes/no codes: Yes first, No second, others by value, sentinels last
    if (s.type === 'integer' || s.type === 'numeric') {
      ft.sort((a, b) => {
        const na = Number(a[0]), nb = Number(b[0]);
        const sentA = na === 999 || na === 9999;
        const sentB = nb === 999 || nb === 9999;
        if (sentA && !sentB) return 1;
        if (!sentA && sentB) return -1;
        return na - nb;
      });
    } else if (s.codes) {
      ft.sort((a, b) => {
        const priority = code => {
          const lbl = (s.codes[code] || '').toLowerCase();
          const n = Number(code);
          if (lbl.startsWith('yes')) return 0;
          if (lbl.startsWith('no'))  return 1;
          if (n === 999 || n === 9999) return 9999;
          return 2 + n;
        };
        return priority(a[0]) - priority(b[0]);
      });
    }

    const labels = ft.map(([code]) => {
      const label = s.codes && s.codes[code] ? s.codes[code] : code;
      return label.length > 22 ? label.slice(0, 22) + '…' : label;
    });

    const counts = ft.map(([,n]) => n);
    const bgColors = ft.map((_, i) => PALETTE[i % PALETTE.length] + 'cc');
    const bdColors = ft.map((_, i) => PALETTE[i % PALETTE.length]);

    if (chartMode === 'pie') {
      const total = counts.reduce((a, b) => a + b, 0);
      mainChart = new Chart(ctx, {
        type: 'pie',
        data: { labels, datasets: [{ data: counts, backgroundColor: bgColors, borderColor: bdColors, borderWidth: 2 }] },
        options: {
          maintainAspectRatio: false,
          plugins: {
            legend: { position: 'right', labels: { font: { size: 12 }, padding: 14 } },
            tooltip: { callbacks: { label: c =>
              ` ${c.label}: ${c.parsed} (${((c.parsed/total)*100).toFixed(1)}%)`
            }}
          }
        }
      });
    } else {
      mainChart = new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets: [{ label: 'Count', data: counts, backgroundColor: bgColors, borderColor: bdColors, borderWidth: 1 }] },
        options: {
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { maxRotation: 45 } },
            y: { title: { display: true, text: 'Count' }, beginAtZero: true }
          }
        }
      });
    }
  }
}

// ── Missingness tab ────────────────────────────────────────────────────────
function renderMissingness(data) {
  data = data || cohortData;
  if (missChart) { missChart.destroy(); missChart = null; }
  const ctx = $('miss-chart').getContext('2d');
  const keys = Object.keys(SCHEMA).filter(k => k !== 'R0_TCode');
  const n = data.length;

  const rows = keys.map(k => {
    const vals = data.map(r => r[k]);
    const nullPct  = vals.filter(v => v === null || v === undefined).length / n * 100;
    const s = SCHEMA[k];
    const sentPct = s.sentinel ? vals.filter(v => v === s.sentinel || v === 9999).length / n * 100 : 0;
    return { key: k, nullPct, sentPct };
  }).sort((a, b) => b.nullPct - a.nullPct);

  missChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: rows.map(r => r.key),
      datasets: [
        { label: 'Missing (null %)', data: rows.map(r => r.nullPct),
          backgroundColor: '#6B58A0cc', borderWidth: 0 },
        { label: 'Sentinel NA (%)',   data: rows.map(r => r.sentPct),
          backgroundColor: '#B9E05Daa', borderWidth: 0 },
      ]
    },
    options: {
      indexAxis: 'y',
      plugins: { legend: { position: 'top' } },
      scales: {
        x: { stacked: true, max: 100, title: { display: true, text: '% of records' } },
        y: { stacked: true, ticks: { font: { size: 10 } } }
      },
      maintainAspectRatio: false,
    }
  });
}

// ── Stratified tab ─────────────────────────────────────────────────────────
function populateStratSelects() {
  const catVars = Object.keys(SCHEMA).filter(k => {
    const s = SCHEMA[k];
    return (s.type === 'categorical' || s.type === 'binary') && s.codes && k !== 'R0_TCode';
  });
  const numVars = Object.keys(SCHEMA).filter(k => {
    const s = SCHEMA[k];
    return s.type === 'numeric' || s.type === 'integer';
  });

  const byOpts = catVars.map(k => `<option value="${k}">${k}</option>`).join('');
  const tgtOpts = numVars.map(k => `<option value="${k}">${k}</option>`).join('');

  $('strat-by').innerHTML     = byOpts;
  $('strat-target').innerHTML = tgtOpts;
  $('t1-strat-by').innerHTML  = '<option value="">None (whole cohort)</option>' + byOpts;
}

function renderStratified() {
  const byKey  = $('strat-by').value;
  const tgtKey = $('strat-target').value;
  if (!byKey || !tgtKey) return;

  const s   = SCHEMA[byKey];
  const groups = {};
  cohortData.forEach(r => {
    const gv = r[byKey];
    if (gv === null || gv === undefined) return;
    const gk = String(gv);
    if (!groups[gk]) groups[gk] = [];
    groups[gk].push(r);
  });

  const area = $('strat-charts-area');
  area.innerHTML = '';

  const PALETTE = ['#6B58A0','#B9E05D','#A599C7','#d46c00'];

  Object.entries(groups).sort().slice(0, 8).forEach(([gval, gdata], idx) => {
    const label = (s.codes && s.codes[gval]) ? s.codes[gval] : gval;
    const values = getValidValues(tgtKey, gdata).map(Number).filter(n => !isNaN(n));
    if (!values.length) return;

    const div = document.createElement('div');
    div.className = 'card';

    const tgt_s = SCHEMA[tgtKey];
    const ss = summarise(values);
    const min = Math.min(...values), max = Math.max(...values);
    const bins = 20, step = (max - min) / bins || 1;
    const buckets = Array(bins).fill(0);
    const binLabels = Array.from({length:bins}, (_,i) => fmt(min + i*step, 1));
    values.forEach(v => {
      const bi = Math.min(bins-1, Math.floor((v - min)/step));
      buckets[bi]++;
    });

    div.innerHTML = `
      <div class="card-title" style="color:${PALETTE[idx%PALETTE.length]}">${byKey} = ${label}
        <span class="badge" style="background:${PALETTE[idx%PALETTE.length]}">${gdata.length}</span></div>
      <div style="font-size:11px;color:var(--muted);margin-bottom:8px">
        Mean ${fmt(ss.mean)} (SD ${fmt(ss.sd)}) | Median ${fmt(ss.median)} | Range ${fmt(ss.min)}–${fmt(ss.max)}</div>
      <div style="position:relative;height:180px;">
        <canvas id="sc-${idx}"></canvas>
      </div>`;
    area.appendChild(div);

    // Render synchronously now that the card is in the DOM — avoids layout shifts
    const ctx2 = document.getElementById(`sc-${idx}`).getContext('2d');
    new Chart(ctx2, {
      type: 'bar',
      data: { labels: binLabels, datasets: [{
        label: tgtKey, data: buckets,
        backgroundColor: PALETTE[idx%PALETTE.length] + '99',
        borderColor: PALETTE[idx%PALETTE.length], borderWidth: 1
      }]},
      options: { plugins:{ legend:{ display:false } },
        scales:{ x:{ ticks:{ maxRotation:45, font:{size:9} } },
                 y:{ title:{ display:true, text:'Count' }, beginAtZero:true } },
        maintainAspectRatio: false }
    });
  });
}

// ── Cohort builder ─────────────────────────────────────────────────────────
let filterIdCounter = 0;

function addFilter() {
  const id = filterIdCounter++;
  filters.push({ id, field: 'R0_BMI', operator: '>', value: '', logic: 'AND' });
  renderFilterRows();
}

function removeFilter(id) {
  filters = filters.filter(f => f.id !== id);
  renderFilterRows();
  applyFilters();
}

function renderFilterRows() {
  const cont = $('filter-rows');
  cont.innerHTML = '';

  const numVars = Object.keys(SCHEMA).filter(k => ['numeric','integer'].includes(SCHEMA[k].type) && k !== 'R0_TCode');
  const catVars = Object.keys(SCHEMA).filter(k => ['categorical','binary'].includes(SCHEMA[k].type) && k !== 'R0_TCode');
  const allVars = [...numVars, ...catVars].sort();

  filters.forEach((f, idx) => {
    const s = SCHEMA[f.field] || {};
    const isNum = ['numeric','integer'].includes(s.type);
    const isCat = ['categorical','binary'].includes(s.type);

    const row = document.createElement('div');
    row.className = 'filter-row';

    // Logic (AND/OR) — first filter has no logic
    const logicHtml = idx === 0 ? '<span style="min-width:50px;font-size:12px;color:var(--muted)">WHERE</span>'
      : `<select class="filter-logic" onchange="App.updateFilter(${f.id},'logic',this.value)">
           <option value="AND" ${f.logic==='AND'?'selected':''}>AND</option>
           <option value="OR"  ${f.logic==='OR'?'selected':''}> OR</option>
         </select>`;

    // Operator options based on type
    let opHtml = '';
    if (isNum) {
      opHtml = `<select class="filter-op" onchange="App.updateFilter(${f.id},'operator',this.value)">
        ${['>','>=','<','<=','=','!='].map(op => `<option ${f.operator===op?'selected':''}>${op}</option>`).join('')}
      </select>`;
    } else if (isCat && s.codes) {
      opHtml = `<select class="filter-op" style="width:60px" onchange="App.updateFilter(${f.id},'operator',this.value)">
        <option value="in" ${f.operator==='in'?'selected':''}>is</option>
        <option value="not_in" ${f.operator==='not_in'?'selected':''}>is not</option>
      </select>`;
    }

    // Value input
    let valHtml = '';
    if (isNum) {
      valHtml = `<input type="number" step="any" value="${f.value}" placeholder="value"
        onchange="App.updateFilter(${f.id},'value',this.value)"
        oninput="App.updateFilter(${f.id},'value',this.value)" />`;
    } else if (isCat && s.codes) {
      valHtml = `<select class="filter-op" onchange="App.updateFilter(${f.id},'value',this.value)">
        ${Object.entries(s.codes).map(([code, label]) =>
          `<option value="${code}" ${String(f.value)===String(code)?'selected':''}>${code}: ${label}</option>`
        ).join('')}
      </select>`;
    }

    row.innerHTML = `
      ${logicHtml}
      <select onchange="App.updateFilter(${f.id},'field',this.value)">
        ${allVars.map(k => `<option value="${k}" ${f.field===k?'selected':''}>${k}</option>`).join('')}
      </select>
      ${opHtml}
      ${valHtml}
      <button class="btn-remove" onclick="App.removeFilter(${f.id})" title="Remove">×</button>
    `;
    cont.appendChild(row);
  });

  if (!filters.length) {
    cont.innerHTML = '<p style="color:var(--muted);font-size:13px;padding:8px">No filters applied — showing full dataset.</p>';
  }
}

function updateFilter(id, field, value) {
  const f = filters.find(x => x.id === id);
  if (!f) return;
  f[field] = value;
  // When field changes, reset operator/value defaults
  if (field === 'field') {
    const s = SCHEMA[value];
    if (['numeric','integer'].includes(s.type)) { f.operator = '>'; f.value = ''; }
    else { f.operator = 'in'; f.value = Object.keys(s.codes || {})[0] || ''; }
    renderFilterRows();
  }
  applyFilters();
}

function applyFilters() {
  if (!filters.length) {
    cohortData = rawData;
    updateCohortUI();
    return;
  }

  // Evaluate each filter against each record
  cohortData = rawData.filter(row => {
    let result = null;
    filters.forEach((f, idx) => {
      const rv = row[f.field];
      let match = false;

      if (rv === null || rv === undefined) {
        match = false;
      } else if (f.operator === 'in') {
        match = String(rv) === String(f.value);
      } else if (f.operator === 'not_in') {
        match = String(rv) !== String(f.value);
      } else {
        const nv = Number(rv), fv = Number(f.value);
        if (isNaN(nv) || isNaN(fv)) { match = false; }
        else {
          if (f.operator === '>')  match = nv > fv;
          if (f.operator === '>=') match = nv >= fv;
          if (f.operator === '<')  match = nv < fv;
          if (f.operator === '<=') match = nv <= fv;
          if (f.operator === '=')  match = nv === fv;
          if (f.operator === '!=') match = nv !== fv;
        }
      }

      if (idx === 0) { result = match; }
      else if (f.logic === 'OR')  { result = result || match; }
      else                         { result = result && match; }
    });
    return result;
  });

  updateCohortUI();
}

function updateCohortUI() {
  $('cohort-count').textContent = cohortData.length;
  buildAttritionFlow();
  renderCohortPreview();
  if (activeVar) renderVarDetail(activeVar, cohortData);
  renderMissingness(cohortData);
  $('miss-cohort-label').textContent = cohortData.length < rawData.length
    ? `Cohort (n=${cohortData.length})` : 'Full dataset';
}

function buildAttritionFlow() {
  const cont = $('attrition-flow');
  const n    = rawData.length;
  const nc   = cohortData.length;
  const excl = n - nc;

  let html = `
    <div class="attrition-step total">
      <span class="att-n">${n}</span>
      <span class="att-label">Total participants in dataset</span>
    </div>`;

  filters.forEach((f, i) => {
    const s = SCHEMA[f.field];
    const label = s.codes && s.codes[f.value] ? s.codes[f.value] : f.value;
    // Apply filters up to and including i
    const partial = rawData.filter(row => {
      let result = null;
      filters.slice(0, i + 1).forEach((ff, idx) => {
        const rv = row[ff.field];
        let match = false;
        if (rv !== null && rv !== undefined) {
          if (ff.operator === 'in')     match = String(rv) === String(ff.value);
          else if (ff.operator === 'not_in') match = String(rv) !== String(ff.value);
          else {
            const nv = Number(rv), fv = Number(ff.value);
            if (!isNaN(nv) && !isNaN(fv)) {
              if (ff.operator === '>')  match = nv > fv;
              if (ff.operator === '>=') match = nv >= fv;
              if (ff.operator === '<')  match = nv < fv;
              if (ff.operator === '<=') match = nv <= fv;
              if (ff.operator === '=')  match = nv === fv;
              if (ff.operator === '!=') match = nv !== fv;
            }
          }
        }
        if (idx === 0) result = match;
        else if (ff.logic === 'OR') result = result || match;
        else result = result && match;
      });
      return result;
    }).length;
    const prev = i === 0 ? n : rawData.filter(row => {
      let r = null;
      filters.slice(0, i).forEach((ff, idx) => {
        const rv = row[ff.field]; let m = false;
        if (rv !== null && rv !== undefined) {
          if (ff.operator==='in') m=String(rv)===String(ff.value);
          else if(ff.operator==='not_in') m=String(rv)!==String(ff.value);
          else { const nv=Number(rv),fv=Number(ff.value); if(!isNaN(nv)&&!isNaN(fv)){if(ff.operator==='>') m=nv>fv; if(ff.operator==='>=') m=nv>=fv; if(ff.operator==='<') m=nv<fv; if(ff.operator==='<=') m=nv<=fv; if(ff.operator==='=') m=nv===fv; if(ff.operator==='!=') m=nv!==fv;}}
        }
        if(idx===0) r=m; else if(ff.logic==='OR') r=r||m; else r=r&&m;
      });
      return r;
    }).length;
    const drop = prev - partial;
    const logic = i === 0 ? '' : `${f.logic} `;
    html += `
      <div style="margin:0 0 0 20px;border-left:2px dashed var(--border);padding-left:12px">
        <div class="attrition-step included">
          <span class="att-n">${partial}</span>
          <span class="att-label">${logic}${f.field} ${f.operator} ${label}</span>
          <span class="att-pct" style="color:var(--warn)">−${drop}</span>
        </div>
      </div>`;
  });

  if (filters.length) {
    html += `
      <div class="attrition-step included" style="margin-top:8px">
        <span class="att-n">${nc}</span>
        <span class="att-label">Final cohort</span>
        <span class="att-pct" style="color:var(--green)">${pct(nc, n)} retained</span>
      </div>`;
  }

  cont.innerHTML = html;
  $('cohort-preview-n').textContent = cohortData.length;
}

function renderCohortPreview() {
  const cont = $('cohort-preview-stats');

  // Show only the variables that are actually used in the active filters
  const previewVars = [...new Set(filters.map(f => f.field).filter(Boolean))];

  if (!previewVars.length) {
    cont.innerHTML = '<div style="color:var(--muted);font-size:12px;padding:8px 0">Add filters above to see a summary here.</div>';
    return;
  }

  cont.innerHTML = previewVars.map(k => {
    const s = SCHEMA[k];
    if (!s) return '';
    const vals = getValidValues(k, cohortData);
    let summary = '';
    if (['numeric','integer'].includes(s.type)) {
      const ss = summarise(vals.map(Number));
      summary = ss ? `Mean ${fmt(ss.mean)} (SD ${fmt(ss.sd)})` : '—';
    } else {
      const ft = freqTable(vals.map(String));
      const top = ft.slice(0, 3).map(([code, cnt]) => {
        const label = s.codes && s.codes[code] ? s.codes[code] : code;
        return `${label}: ${cnt}`;
      });
      summary = top.join(' | ');
    }
    return `<div style="padding:8px 0;border-bottom:1px solid var(--border)">
      <div style="font-size:11px;font-weight:700;color:var(--navy)">${k}
        <span style="font-weight:400;color:var(--muted)"> — ${s.desc || ''}</span></div>
      <div style="font-size:12px;color:var(--muted)">${summary}</div>
    </div>`;
  }).join('');
}

function clearFilters() {
  filters = [];
  renderFilterRows();
  applyFilters();
}

// ── Shared branded PNG export ───────────────────────────────────────────────
// Captures `element` with html2canvas, prepends a purple branded header,
// and triggers a download of `filename`.  `hideEl` (optional) is temporarily
// hidden during capture (e.g. a download button inside the card).
function exportBrandedPNG(element, filename, hideEl) {
  if (hideEl) hideEl.style.visibility = 'hidden';

  const xhrDataURL = src => new Promise(resolve => {
    const xhr = new XMLHttpRequest();
    xhr.open('GET', src, true);
    xhr.responseType = 'blob';
    xhr.onload = () => {
      const reader = new FileReader();
      reader.onload  = () => resolve(reader.result);
      reader.onerror = () => resolve(null);
      reader.readAsDataURL(xhr.response);
    };
    xhr.onerror = () => resolve(null);
    xhr.send();
  });

  const imgFromDataURL = dataURL => new Promise(resolve => {
    if (!dataURL) { resolve(null); return; }
    const img = new Image();
    img.onload  = () => resolve(img);
    img.onerror = () => resolve(null);
    img.src = dataURL;
  });

  Promise.all([
    html2canvas(element, { backgroundColor: '#ffffff', scale: 2, useCORS: true, logging: false }),
    xhrDataURL('logo-g.png').then(imgFromDataURL),
    xhrDataURL('logo-white.png').then(imgFromDataURL),
  ]).then(([snapshot, logoG, logoW]) => {
    if (hideEl) hideEl.style.visibility = '';

    const SCALE = 2;
    const W     = snapshot.width;
    const HDRH  = 84 * SCALE;
    const PAD   = 16 * SCALE;

    const out  = document.createElement('canvas');
    out.width  = W;
    out.height = HDRH + snapshot.height;
    const ctx2 = out.getContext('2d');

    // Purple header background
    ctx2.fillStyle = '#6B58A0';
    ctx2.fillRect(0, 0, W, HDRH);

    // Logo-G icon
    let x = PAD;
    const iconH = 40 * SCALE;
    if (logoG) {
      const iconW = Math.round(logoG.naturalWidth * iconH / logoG.naturalHeight);
      ctx2.drawImage(logoG, x, (HDRH - iconH) / 2, iconW, iconH);
      x += iconW + 8 * SCALE;
    }

    // White wordmark
    const wordH = 24 * SCALE;
    if (logoW) {
      const wordW = Math.round(logoW.naturalWidth * wordH / logoW.naturalHeight);
      ctx2.drawImage(logoW, x, (HDRH - wordH) / 2, wordW, wordH);
    }

    // Right-aligned metadata
    const dateStr   = new Date().toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
    const dataLabel = dataFileName   || 'synthetic_data.json';
    const schLabel  = schemaFileName || (schemaSource === 'inferred' ? 'Auto-detected' : schemaSource === 'builtin' ? 'Built-in' : 'unknown');

    ctx2.textAlign    = 'right';
    ctx2.textBaseline = 'top';
    const lineH  = 15 * SCALE;
    const tyBase = (HDRH - 4 * lineH) / 2;

    ctx2.fillStyle = '#B9E05D';
    ctx2.font      = `bold ${12 * SCALE}px Arial, sans-serif`;
    ctx2.fillText('The Generations Study', W - PAD, tyBase);

    ctx2.fillStyle = 'rgba(255,255,255,0.9)';
    ctx2.font      = `${11 * SCALE}px Arial, sans-serif`;
    ctx2.fillText(`Date: ${dateStr}`,    W - PAD, tyBase + lineH);
    ctx2.fillText(`Data: ${dataLabel}`,  W - PAD, tyBase + 2 * lineH);
    ctx2.fillText(`Schema: ${schLabel}`, W - PAD, tyBase + 3 * lineH);

    // Lime divider
    ctx2.strokeStyle = '#B9E05D';
    ctx2.lineWidth   = 2 * SCALE;
    ctx2.beginPath();
    ctx2.moveTo(0, HDRH);
    ctx2.lineTo(W, HDRH);
    ctx2.stroke();

    // Content
    ctx2.drawImage(snapshot, 0, HDRH);

    const a = document.createElement('a');
    a.href     = out.toDataURL('image/png');
    a.download = filename;
    a.click();
    toast(`Downloaded ${filename}`);
  }).catch(err => {
    if (hideEl) hideEl.style.visibility = '';
    console.error('PNG download failed:', err);
    toast('Download failed — see console for details');
  });
}

function downloadChartPNG() {
  if (!mainChart) { toast('No chart to download'); return; }
  const card = document.querySelector('#var-detail > .card');
  if (!card) { toast('No chart to download'); return; }
  const modeName = { histogram: 'histogram', violin: 'violin', bar: 'barchart', pie: 'piechart' }[chartMode] || chartMode;
  const filename  = `${activeVar || 'chart'}_${modeName}.png`;
  const dlBtn = card.querySelector('.btn-download-png');
  exportBrandedPNG(card, filename, dlBtn);
}

function exportAttritionPNG() {
  const card = document.querySelector('#attrition-flow')?.closest('.card');
  if (!card) { toast('No attrition flow to export'); return; }
  exportBrandedPNG(card, 'attrition_flow.png');
}

function exportCohort() {
  const def = {
    meta: { created: new Date().toISOString(), tool: 'Generations Study Demo Dashboard',
      dataset: 'synthetic_data.json', total_n: rawData.length, cohort_n: cohortData.length },
    filters: filters.map(({field, operator, value, logic}) => ({field, operator, value, logic})),
    cohort_tcodes: cohortData.map(r => r.R0_TCode)
  };
  const blob = new Blob([JSON.stringify(def, null, 2)], {type:'application/json'});
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href = url; a.download = `cohort_${Date.now()}.json`;
  a.click(); URL.revokeObjectURL(url);
  toast('Cohort definition exported.');
}

// ── Table 1 ────────────────────────────────────────────────────────────────
function renderTable1() {
  const stratKey = $('t1-strat-by').value;
  const n = cohortData.length;
  $('t1-n-badge').textContent = `n=${n}`;
  $('t1-col-all').textContent = `All (n=${n})`;

  let stratGroups = null;
  const s1col = $('t1-col-s1'), s2col = $('t1-col-s2');
  s1col.style.display = 'none'; s2col.style.display = 'none';

  if (stratKey) {
    const sSchema = SCHEMA[stratKey];
    const gmap = {};
    cohortData.forEach(r => {
      const gv = r[stratKey];
      if (gv === null || gv === undefined) return;
      const gk = String(gv);
      if (!gmap[gk]) gmap[gk] = [];
      gmap[gk].push(r);
    });
    stratGroups = Object.entries(gmap).sort();
    if (stratGroups.length >= 1) {
      const [g0, g0data] = stratGroups[0];
      s1col.textContent = `${(sSchema.codes && sSchema.codes[g0]) || g0} (n=${g0data.length})`;
      s1col.style.display = '';
    }
    if (stratGroups.length >= 2) {
      const [g1, g1data] = stratGroups[1];
      s2col.textContent = `${(sSchema.codes && sSchema.codes[g1]) || g1} (n=${g1data.length})`;
      s2col.style.display = '';
    }
  }

  const keys = Object.keys(SCHEMA).filter(k => k !== 'R0_TCode');
  const tbody = $('t1-body');
  tbody.innerHTML = '';

  function cellSummary(key, data) {
    const s = SCHEMA[key];
    const vals = getValidValues(key, data);
    if (!vals.length) return '—';

    if (s.type === 'numeric' || s.type === 'integer') {
      const ss = summarise(vals.map(Number));
      return ss ? `${fmt(ss.mean, 1)} (${fmt(ss.sd, 1)})` : '—';
    } else {
      const ft = freqTable(vals.map(String));
      const top = ft.slice(0, 3).map(([code, cnt]) => {
        const label = (s.codes && s.codes[code]) ? s.codes[code] : code;
        const short = label.length > 14 ? label.slice(0,14)+'…' : label;
        return `${short}: ${cnt} (${pct(cnt, data.length)})`;
      });
      return top.join('<br>');
    }
  }

  keys.forEach(key => {
    const s = SCHEMA[key];
    const allVals = cohortData.map(r => r[key]);
    const nullCount = allVals.filter(v => v === null || v === undefined).length;
    const sentVal   = s.sentinel;
    const sentCount = sentVal != null
      ? allVals.filter(v => v === sentVal || v === 9999).length
      : 0;
    const tr = document.createElement('tr');

    let stratCells = '';
    if (stratGroups) {
      const [g0data] = stratGroups[0] ? [stratGroups[0][1]] : [[]];
      const [g1data] = stratGroups[1] ? [stratGroups[1][1]] : [[]];
      stratCells = `
        <td>${cellSummary(key, g0data)}</td>
        <td>${stratGroups[1] ? cellSummary(key, g1data) : ''}</td>`;
    } else {
      // Hidden placeholders must match the two hidden <th> cells in the header;
      // without these the Null/Sentinel columns mis-align under the hidden ths.
      stratCells = '<td style="display:none"></td><td style="display:none"></td>';
    }

    const sentLabel = sentVal != null
      ? `<span class="t1-missing" title="${sentVal} = Not Applicable">${pct(sentCount, cohortData.length)}</span>
         <span style="font-size:10px;color:var(--muted);display:block">(${sentVal}=NA)</span>`
      : '<span style="color:var(--border)">—</span>';

    tr.innerHTML = `
      <td><strong>${key}</strong><br><span class="t1-type">${s.desc.slice(0,50)}${s.desc.length>50?'…':''}</span></td>
      <td><span class="var-type-pill ${typeClass(s.type)}">${s.type}</span></td>
      <td>${cellSummary(key, cohortData)}</td>
      ${stratCells}
      <td><span class="t1-missing">${pct(nullCount, cohortData.length)}</span></td>
      <td>${sentLabel}</td>
    `;
    tbody.appendChild(tr);
  });
}

function exportTable1CSV() {
  const rows = [['Variable','Type','Summary (n/mean±SD)','Null (%)','NA Sentinel (%)','Sentinel value']];
  const keys = Object.keys(SCHEMA).filter(k => k !== 'R0_TCode');
  keys.forEach(key => {
    const s = SCHEMA[key];
    const vals = getValidValues(key, cohortData);
    const allVals = cohortData.map(r => r[key]);
    const nullPct = pct(allVals.filter(v => v === null || v === undefined).length, cohortData.length);
    const sentVal = s.sentinel;
    const sentPct = sentVal != null
      ? pct(allVals.filter(v => v === sentVal || v === 9999).length, cohortData.length)
      : '—';
    let summary = '';
    if (['numeric','integer'].includes(s.type)) {
      const ss = summarise(vals.map(Number));
      summary = ss ? `${fmt(ss.mean,1)} (SD ${fmt(ss.sd,1)})` : '';
    } else {
      const ft = freqTable(vals.map(String));
      summary = ft.slice(0,3).map(([code,cnt]) => `${code}:${cnt}`).join('; ');
    }
    rows.push([key, s.type, summary, nullPct, sentPct, sentVal != null ? String(sentVal) : '']);
  });
  const csv = rows.map(r => r.map(v => `"${String(v).replace(/"/g,'""')}"`).join(',')).join('\n');
  const blob = new Blob([csv], {type:'text/csv'});
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a'); a.href=url; a.download='table1.csv'; a.click();
  URL.revokeObjectURL(url);
  toast('Table 1 exported as CSV.');
}

// ── Public API ─────────────────────────────────────────────────────────────
window.App = {
  addFilter, removeFilter, updateFilter, clearFilters,
  exportCohort, exportAttritionPNG, renderStratified, renderTable1, exportTable1CSV,
  downloadChartPNG, continueToApp, reloadSchema,
};

// ── Bootstrap ──────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  setupDrop();

  // Tab clicks
  document.querySelectorAll('.nav-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      const name = tab.dataset.tab;
      showTab(name);
      if (name === 'table1') renderTable1();
      if (name === 'missingness') renderMissingness(cohortData);
    });
  });

  // Sidebar search
  $('var-search').addEventListener('input', renderVarList);

  // Group filter buttons
  $('group-filter').addEventListener('click', e => {
    const btn = e.target.closest('.gf-btn');
    if (!btn) return;
    document.querySelectorAll('.gf-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    activeGroup = btn.dataset.group;
    renderVarList();
  });
});
