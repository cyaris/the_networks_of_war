<script>
  import { forceCenter, forceCollide, forceLink, forceManyBody, forceSimulation, forceX, forceY } from "d3-force"
  import { scaleLinear } from "d3-scale"
  import pluralize from "pluralize"
  import { onDestroy } from "svelte"
  import { CheckboxFilter, InfoIcon, Select } from "svelte-lib/components"

  import graphData from "../static/graphData.json"
  import dataDictionary from "../static/metricDataDictionary.json"

  let wars = graphData.wars
  let graphsByWarId = graphData.graphsByWarId

  const timeframeItems = [
    { value: "first_year", label: "First Year" },
    { value: "last_year", label: "Last Year" },
    { value: "all_years", label: "All Years" }
  ]
  const noTimeframeItemsMessage = "No timeframe data available."
  const noNodeSizeItemsMessage = "No node size data available."
  const noLinkDashItemsMessage = "No link dash data available."
  const metricDictionary = dataDictionary.metrics || {}
  const controlTooltips = dataDictionary.controls || {}

  function plural(value, noun) {
    let parsed = Number(value || 0)

    return `${parsed.toLocaleString()} ${pluralize(noun, parsed)}`
  }

  function warSecondaryLabel(war) {
    return `${plural(war.total_participants, "participant")}, ${plural(war.total_dyads, "dyad")}`
  }

  function graphForWar(war) {
    return graphsByWarId[String(war?.war_id)] || { nodes: [], links: [] }
  }

  function linkDashFieldCount(war) {
    let graph = graphForWar(war)
    let fields = Array.from(
      new Set((graph.links || []).flatMap(link => timeframeItems.flatMap(item => Object.keys(link[item.value] || {}))))
    )

    return fields.filter(field =>
      graph.links.some(link => timeframeItems.some(item => Number(link[item.value]?.[field]) > 0))
    ).length
  }

  function preferredWarItem(items) {
    return items.find(item => item.linkDashFieldCount > 0) || items[0]
  }

  function syncSelectValue(key, items, fallbackItem = null) {
    let currentValue = selectValue[key]

    if (!items.length) {
      if (currentValue) selectValue[key] = null
      return
    }

    let matchingItem = items.find(item => item.value == currentValue?.value)
    let nextValue = matchingItem || fallbackItem

    if (currentValue !== nextValue) {
      selectValue[key] = nextValue
    }
  }

  function syncWarSelectValue(items) {
    syncSelectValue("war", items, preferredWarItem(items))
  }

  function emptyNodeMargins() {
    return {
      name: {},
      radius_size: {},
      vertical_name_shift: {},
      name_fits_in_node: {},
      horizontal_name_shift: {},
      name_lengths: {},
      added_top_margin: {},
      added_bottom_margin: {},
      added_left_margin: {},
      added_right_margin: {}
    }
  }

  let countryFiltersByCCode = {}

  Object.values(graphsByWarId).forEach(graph => {
    let graphNodes = graph.nodes || []

    graphNodes.forEach(node => {
      if (Number(node.c_code) <= 0) return

      let cCode = String(node.c_code)

      if (!countryFiltersByCCode[cCode]) {
        countryFiltersByCCode[cCode] = { c_code: node.c_code, names: new Set(), warIds: new Set() }
      }

      countryFiltersByCCode[cCode].names.add(node.participant)
      countryFiltersByCCode[cCode].warIds.add(String(node.war_id))
    })
  })

  let linkDashFieldCountsByWarId = Object.fromEntries(wars.map(war => [String(war.war_id), linkDashFieldCount(war)]))
  let warTypeItems = Array.from(new Set(wars.map(war => war.war_type)))
    .sort()
    .map(warType => ({
      value: warType,
      label: warType
    }))
  let selectItems = {
    country: Object.values(countryFiltersByCCode)
      .map(country => {
        let label = Array.from(country.names).sort((a, b) => a.localeCompare(b))[0]

        return {
          value: String(country.c_code),
          label,
          selectedLabel: label,
          secondaryLabel: plural(country.warIds.size, "war"),
          c_code: country.c_code,
          warIds: country.warIds
        }
      })
      .sort((a, b) => a.label.localeCompare(b.label)),
    war: [],
    timeframe: timeframeItems,
    nodeDescriptor: [],
    linkDescriptor: []
  }
  let selectValue = {
    country: null,
    war: null,
    timeframe: timeframeItems[2],
    nodeDescriptor: null,
    linkDescriptor: null
  }
  let selectedWarTypes = warTypeItems.map(item => item.value)
  let deselectedWarTypes = []
  let graph = { nodes: [], links: [] }
  let selectedWar = null
  let pageWidth

  let width = 900
  let simulation
  let svg
  let nodes = []
  let links = []
  let linkNodes = []
  let descriptorNodes = []
  let descriptorLinks = []
  let hoverNode = null
  let dragNode = null
  let tooltip = null
  let tooltipWidth = 320
  let tooltipHeight = 96
  let currentGraph = null
  let currentWidth = null
  let currentSizingSignature = null
  let currentLinkDescriptorSignature = null
  let linkDashPulseTimer = null
  let linkDashStrokeWidth = 3
  let nodeSizingValues = []
  let nodeSizingById = {}
  let maxDomain = 2
  let stdNullRadiusSize = 1
  let nodeMargins = emptyNodeMargins()
  let radiusScale = scaleLinear([0, maxDomain], [1, 125])
  let primaryNode = null

  const graphTextSize = 12
  const height = 700
  const minRadiusSize = 1
  const maxRadiusSize = 125
  const dyadMinLinkDistance = 380
  const linkNodeSize = 2.5
  const nodeStrokeWidth = 1
  const nodeSizeWarningOffset = 10
  const nodeSizeWarningLabelGap = 14
  const maxVisibleNodeSizeWarnings = 6
  const graphCenterY = height * 0.5
  const addedMarginSize = Math.max(linkNodeSize, 10)
  const tooltipOffset = 16
  const tooltipPadding = 8
  const compactNumberUnits = [
    { value: 1_000_000_000_000, label: "trillion" },
    { value: 1_000_000_000, label: "billion" },
    { value: 1_000_000, label: "million" }
  ]
  const compactNumberMinimum = 1_000_000
  const tooltipFractionDigits = 2
  const alwaysShowZeroMetricFields = new Set(["battle_deaths", "battle_deaths_per_day"])
  const metricSuffixOmittedByField = new Set([
    "allied_countries",
    "arms_technologies_used",
    "battle_deaths",
    "concurrent_wars",
    "days_at_war",
    "mid_dyads",
    "mid_dyads_initiated",
    "mid_dyads_joined",
    "mid_dyads_targeted",
    "terrorism_deaths",
    "trade_countries"
  ])
  const pluralizedMetricSuffixes = new Set([
    "coal-ton equivalents",
    "countries",
    "days",
    "deaths",
    "dyads",
    "people",
    "technologies",
    "tons",
    "wars"
  ])
  const sideColors = { 1: "#2f7f66", 2: "#b54f72", 3: "#5f70b8", null: "#71717a", undefined: "#71717a" }

  $: graphCenterX = width * 0.5
  $: selectedWarTypeValues = selectedWarTypes?.length ? selectedWarTypes : []
  $: selectedCountryWarIds = selectValue.country?.warIds
  $: filteredWars = wars.filter(
    war =>
      selectedWarTypeValues.includes(war.war_type) &&
      (!selectedCountryWarIds || selectedCountryWarIds.has(String(war.war_id)))
  )
  $: {
    let nextWarItems = filteredWars.map(war => ({
      value: String(war.war_id),
      label: war.war_name,
      selectedLabel: war.war_name,
      secondaryLabel: warSecondaryLabel(war),
      war_type: war.war_type,
      linkDashFieldCount: linkDashFieldCountsByWarId[String(war.war_id)] || 0,
      war
    }))

    selectItems.war = nextWarItems
    syncWarSelectValue(nextWarItems)
  }
  $: selectedWar = selectValue.war?.war
  $: graph = selectedWar ? graphForWar(selectedWar) : { nodes: [], links: [] }

  function numberValue(value) {
    if (value == null || value === "") return null

    let parsed = Number(value)

    return Number.isFinite(parsed) ? parsed : null
  }

  function displayExactNumber(value) {
    return value.toLocaleString("en-US", { maximumFractionDigits: tooltipFractionDigits })
  }

  function displayCompactNumber(value) {
    let absoluteValue = Math.abs(value)

    if (absoluteValue < compactNumberMinimum) return displayExactNumber(value)

    let unit = compactNumberUnits.find(({ value: unitValue }) => absoluteValue >= unitValue)

    if (!unit) return displayExactNumber(value)

    let roundedScaledValue =
      Math.round((value / unit.value) * 10 ** tooltipFractionDigits) / 10 ** tooltipFractionDigits
    let compactValue = roundedScaledValue.toLocaleString("en-US", {
      maximumFractionDigits: tooltipFractionDigits
    })

    return `${compactValue} ${unit.label}`
  }

  function displayMetricSuffix(value, field, suffix) {
    if (!suffix || metricSuffixOmittedByField.has(field)) return ""
    if (!pluralizedMetricSuffixes.has(suffix)) return suffix

    return pluralize(suffix, value)
  }

  function displayMetricNumber(value, field) {
    let parsed = numberValue(value)

    if (parsed == null) return "Unknown"

    let metric = metricDictionary[field] || {}
    let prefix = metric.valuePrefix || ""
    let suffix = displayMetricSuffix(parsed, field, metric.valueSuffix || "")
    let suffixSeparator = suffix && !suffix.startsWith("%") ? " " : ""

    return `${prefix}${displayCompactNumber(parsed)}${suffix ? `${suffixSeparator}${suffix}` : ""}`
  }

  function displayDate(value, estimated = false) {
    if (!value) return "Unknown"

    let formatted = String(value)

    return `${formatted}${Number(estimated) == 1 ? " (estimated)" : ""}`
  }

  function finiteValues(values) {
    return values.filter(Number.isFinite)
  }

  function hasPositiveFiniteValue(values) {
    return values.some(value => Number.isFinite(value) && value > 0)
  }

  function hasSizingVariation({ values, nullRadiusNodes }) {
    let uniqueValues = Array.from(new Set(finiteValues(values)))

    return uniqueValues.length > 1 || (nullRadiusNodes > 0 && hasPositiveFiniteValue(uniqueValues))
  }

  function averageValue(values) {
    let knownValues = finiteValues(values)

    return knownValues.length ? knownValues.reduce((total, value) => total + value, 0) / knownValues.length : 0
  }

  function fieldLabel(field) {
    return metricDictionary[field]?.label || field.replaceAll("_", " ").replace(/\b\w/g, value => value.toUpperCase())
  }

  function metricTooltip(field, fallback) {
    let metric = metricDictionary[field]

    if (!metric) return fallback

    return [
      `${metric.label}${metric.unit ? ` (${metric.unit})` : ""}`,
      metric.source ? `Source: ${metric.source}` : "",
      metric.calculation ? `Calculation: ${metric.calculation}` : ""
    ]
      .filter(Boolean)
      .join("\n\n")
  }

  function descriptorFields(rows, timeframe) {
    return Array.from(new Set(rows.flatMap(row => Object.keys(row[timeframe] || {}))))
  }

  function descriptorItems(fields) {
    return fields
      .sort((a, b) => fieldLabel(a).localeCompare(fieldLabel(b)))
      .map(field => ({
        value: field,
        label: fieldLabel(field),
        secondaryLabel: metricDictionary[field]?.unit || field
      }))
  }

  function descriptorValue(row, field, timeframe = selectValue.timeframe?.value || "all_years") {
    return row?.[timeframe]?.[field]
  }

  function dateDays(startDate, endDate) {
    let start = new Date(`${startDate}T00:00:00`)
    let end = endDate ? new Date(`${endDate}T00:00:00`) : new Date()
    let days = Math.round((end - start) / 86400000) + 1

    return Number.isFinite(days) ? Math.max(1, days) : null
  }

  function nodeDescriptorNumericValue(node, field) {
    if (field == "days_at_war") {
      let totalDays = dateDays(node.start_date, Number(node.ongoing_war) == 1 ? null : node.end_date)

      return totalDays == null ? null : totalDays - (numberValue(descriptorValue(node, "days_not_at_war")) ?? 0)
    }

    if (field == "battle_deaths") return numberValue(node.battle_deaths)

    if (field == "battle_deaths_per_day") {
      let totalDays = dateDays(node.start_date, Number(node.ongoing_war) == 1 ? null : node.end_date)
      let daysAtWar =
        totalDays == null ? null : totalDays - (numberValue(descriptorValue(node, "days_not_at_war")) ?? 0)
      let battleDeaths = numberValue(descriptorValue(node, "battle_deaths")) ?? numberValue(node.battle_deaths)

      return Number.isFinite(daysAtWar) && battleDeaths != null
        ? Math.round((battleDeaths / daysAtWar) * 100) / 100
        : null
    }

    return numberValue(descriptorValue(node, field))
  }

  function getNodeDescriptiveValues(sizeField) {
    let values = []
    let byId = {}
    let descriptorMaxDomain = 0
    let descriptorNullRadiusNodes = 0

    descriptorNodes.forEach(node => {
      let sizeValue = nodeDescriptorNumericValue(node, sizeField)

      if (sizeValue == null) {
        descriptorNullRadiusNodes += 1
        byId[node.id] = { value: null, isUnknown: true }
        values.push(NaN)
      } else {
        let value = Math.max(0, sizeValue)

        byId[node.id] = { value, isUnknown: false }
        descriptorMaxDomain += value
        values.push(value)
      }
    })

    let nodeCount = values.length
    let descriptorStdNullRadiusSize

    if (!nodeCount) {
      descriptorMaxDomain = 2
      descriptorStdNullRadiusSize = 1
    } else if (descriptorNullRadiusNodes != nodeCount) {
      descriptorStdNullRadiusSize = descriptorMaxDomain / (nodeCount - descriptorNullRadiusNodes)
      descriptorMaxDomain += descriptorStdNullRadiusSize * descriptorNullRadiusNodes
    } else {
      descriptorMaxDomain = 2
      descriptorStdNullRadiusSize = descriptorMaxDomain / nodeCount
    }

    return {
      values,
      byId,
      maxDomain: descriptorMaxDomain,
      stdNullRadiusSize: descriptorStdNullRadiusSize,
      nullRadiusNodes: descriptorNullRadiusNodes
    }
  }

  function getLinkDescriptiveValues(linkDescriptorName) {
    return descriptorLinks.map(link => {
      let value = Number.parseFloat(descriptorValue(link, linkDescriptorName))

      return Number.isFinite(value) && value > 0 ? 1 : 0
    })
  }

  function nodeFieldItems(rows, timeframe) {
    let fields = descriptorFields(rows, timeframe)

    if (timeframe == "all_years") {
      fields = Array.from(new Set([...fields, "days_at_war", "battle_deaths", "battle_deaths_per_day"]))
    }

    let daysAtWarSizing = getNodeDescriptiveValues("days_at_war")
    let daysNotAtWarSizing = getNodeDescriptiveValues("days_not_at_war")

    return descriptorItems(
      fields.filter(field => {
        let sizing = getNodeDescriptiveValues(field)
        let { values, nullRadiusNodes } = sizing

        if (!hasPositiveFiniteValue(values)) return false
        if (!hasSizingVariation(sizing)) return false
        if (nullRadiusNodes / values.length >= 0.5) return false
        if (field == "days_at_war" && !hasSizingVariation(daysAtWarSizing)) return false
        if (field == "days_not_at_war" && !hasSizingVariation(daysNotAtWarSizing)) return false
        if (field == "battle_deaths_per_day" && !hasSizingVariation(daysAtWarSizing)) return false

        return true
      })
    )
  }

  function linkFieldItems(rows, timeframe) {
    return descriptorItems(
      descriptorFields(rows, timeframe).filter(field => hasPositiveFiniteValue(getLinkDescriptiveValues(field)))
    )
  }

  function enrichedNodes(rawNodes) {
    return rawNodes.map(node => {
      let daysAtWar = dateDays(node.start_date, node.ongoing_war ? null : node.end_date)
      let battleDeaths = numberValue(node.battle_deaths)
      let battleDeathsPerDay =
        Number.isFinite(daysAtWar) && battleDeaths != null ? Math.round((battleDeaths / daysAtWar) * 100) / 100 : null
      let allYearMetrics = {
        ...(node.metrics?.all_years || {}),
        battle_deaths: battleDeaths,
        days_at_war: daysAtWar,
        battle_deaths_per_day: battleDeathsPerDay
      }

      return {
        ...node,
        metrics: {
          ...(node.metrics || {}),
          all_years: allYearMetrics
        },
        all_years: {
          ...(node.all_years || {}),
          battle_deaths: battleDeaths,
          days_at_war: daysAtWar,
          battle_deaths_per_day: battleDeathsPerDay
        }
      }
    })
  }

  function textWidth(value) {
    return String(value || "").length * 6.5
  }

  function nameFitsInNode(nodeRadiusValue, nameInput) {
    return nodeRadiusValue * 2 > textWidth(nameInput) + 15
  }

  function getNodeMargins() {
    let margins = emptyNodeMargins()

    radiusScale = scaleLinear([0, maxDomain], [minRadiusSize, maxRadiusSize])

    nodes.forEach(node => {
      let nodeSizing = nodeSizingById[node.id]
      let nodeIsUnknown = !nodeSizing || nodeSizing.isUnknown
      let nodeHasSelectedDescriptor = Boolean(selectValue.nodeDescriptor?.value)
      let currentRadiusSize = nodeIsUnknown
        ? nodeHasSelectedDescriptor
          ? minRadiusSize
          : radiusScale(stdNullRadiusSize)
        : radiusScale(nodeSizing?.value ?? stdNullRadiusSize)
      let nameFits = nameFitsInNode(currentRadiusSize, node.participant)
      let currentNameLength = textWidth(node.participant)
      let currentNameLengthHalf = currentNameLength / 2
      let currentVerticalShift = nameFits ? 5 : currentRadiusSize + 22.5
      let currentHorizontalShift = nameFits ? 0 : 30

      margins.radius_size[node.id] = currentRadiusSize
      margins.name[node.id] = node.participant
      margins.name_lengths[node.id] = currentNameLength
      margins.vertical_name_shift[node.id] = currentVerticalShift
      margins.horizontal_name_shift[node.id] = currentHorizontalShift
      margins.name_fits_in_node[node.id] = nameFits

      if (!nameFits) {
        margins.added_top_margin[node.id] = currentVerticalShift + addedMarginSize
        margins.added_bottom_margin[node.id] = currentVerticalShift + addedMarginSize
        margins.added_left_margin[node.id] = currentNameLengthHalf + currentHorizontalShift + addedMarginSize
        margins.added_right_margin[node.id] = currentNameLengthHalf + currentHorizontalShift + addedMarginSize
      } else {
        let nodeMargin = Math.max(currentRadiusSize, linkNodeSize) + addedMarginSize

        margins.added_top_margin[node.id] = nodeMargin
        margins.added_bottom_margin[node.id] = nodeMargin
        margins.added_left_margin[node.id] = nodeMargin
        margins.added_right_margin[node.id] = nodeMargin
      }
    })

    return margins
  }

  function nodeRadius(node) {
    return nodeMargins.radius_size[node.id] ?? radiusScale(stdNullRadiusSize)
  }

  function getXAdjusted(id, xLoc) {
    if (id == primaryNode && nodes.length > 2) return graphCenterX

    return Math.max(
      nodeMargins.added_left_margin[id] ?? addedMarginSize,
      Math.min(width - (nodeMargins.added_right_margin[id] ?? addedMarginSize), xLoc ?? graphCenterX)
    )
  }

  function getYAdjusted(id, yLoc) {
    if (id == primaryNode && nodes.length > 2) return graphCenterY

    return Math.max(
      nodeMargins.added_top_margin[id] ?? addedMarginSize,
      Math.min(height - (nodeMargins.added_bottom_margin[id] ?? addedMarginSize), yLoc ?? graphCenterY)
    )
  }

  function linkEndpointId(link, endpoint) {
    return typeof link[endpoint] == "object" ? link[endpoint].id : link[endpoint]
  }

  function identifyPrimaryNode() {
    if (links.length <= 1) return null

    let endpointCounts = new Map()

    links.forEach(link => {
      for (let id of [linkEndpointId(link, "source"), linkEndpointId(link, "target")]) {
        endpointCounts.set(id, (endpointCounts.get(id) || 0) + 1)
      }
    })

    let primaryNodes = Array.from(endpointCounts)
      .filter(([, linkCount]) => linkCount == links.length)
      .map(([id]) => id)

    return primaryNodes.length == 1 ? primaryNodes[0] : null
  }

  function linkX(link, endpoint) {
    return getXAdjusted(linkEndpointId(link, endpoint), link[endpoint]?.x)
  }

  function linkY(link, endpoint) {
    return getYAdjusted(linkEndpointId(link, endpoint), link[endpoint]?.y)
  }

  function labelPosition(node) {
    let id = node.id
    let x = getXAdjusted(id, node.x)
    let y = getYAdjusted(id, node.y)
    let xOperator = 1
    let yOperator = 1
    let yAdjustment = 0

    if (x <= graphCenterX && y >= graphCenterY) {
      xOperator = -1
      yOperator = 1
    } else if (x >= graphCenterX && y <= graphCenterY) {
      xOperator = 1
      yOperator = -1
      yAdjustment = graphTextSize
    } else if (x < graphCenterX && y < graphCenterY) {
      xOperator = -1
      yOperator = -1
      yAdjustment = graphTextSize
    }

    if (nodeMargins.name_fits_in_node[id]) {
      return {
        x: nodeMargins.horizontal_name_shift[id] ?? 0,
        y: nodeMargins.vertical_name_shift[id] ?? 5,
        anchor: "middle",
        inside: true
      }
    }

    return {
      x: (nodeMargins.horizontal_name_shift[id] ?? 30) * xOperator,
      y: (nodeMargins.vertical_name_shift[id] ?? nodeRadius(node) + 22.5) * yOperator + yAdjustment,
      anchor: "middle",
      inside: false
    }
  }

  function nodeSizeWarningPosition(node, label = labelPosition(node)) {
    if (label.inside) {
      let offset = (nodeRadius(node) + nodeSizeWarningOffset) / Math.SQRT2

      return { x: offset, y: -offset }
    }

    let labelDirection = label.x < 0 ? -1 : 1
    let labelWidth = nodeMargins.name_lengths[node.id] ?? textWidth(node.participant)

    return {
      x: label.x + labelDirection * (labelWidth / 2 + nodeSizeWarningLabelGap),
      y: label.y
    }
  }

  function showNodeSizeWarning(node) {
    if (!showNodeSizeWarnings || !selectValue.nodeDescriptor?.value || !hasNodeSizeSignal) return false

    let nodeSizing = nodeSizingById[node.id]

    return !nodeSizing || nodeSizing.isUnknown == true
  }

  function nodeMetricValue(node, metrics, field) {
    let value = displayMetricNumber(metrics[field], field)

    if (field == "battle_deaths" && Number(node.battle_deaths_estimated) == 1) return `${value} (estimated)`
    if (field == "concurrent_wars") return `${value} (avg)`

    return value
  }

  function nodeMetricRows(node) {
    let metrics = node.metrics?.[selectValue.timeframe?.value || "all_years"] || {}

    return Object.keys(metrics)
      .filter(field => {
        let value = numberValue(metrics[field])

        if (field == "days_at_war" || value == null) return false
        if (value != 0 || alwaysShowZeroMetricFields.has(field)) return true

        return field == selectValue.nodeDescriptor?.value
      })
      .sort((a, b) => fieldLabel(a).localeCompare(fieldLabel(b)))
      .map(field => ({
        field,
        label: fieldLabel(field),
        value: nodeMetricValue(node, metrics, field)
      }))
  }

  function linkHasDescriptor(link) {
    return (
      selectValue.linkDescriptor?.value &&
      (numberValue(descriptorValue(link, selectValue.linkDescriptor.value)) ?? 0) > 0
    )
  }

  function refreshGraph() {
    nodes = nodes
    links = links
  }

  function stopSimulation() {
    if (simulation) {
      simulation.stop()
    }
  }

  function applyLegacySizing() {
    let sizing = getNodeDescriptiveValues(selectValue.nodeDescriptor?.value || null)
    nodeSizingValues = sizing.values
    nodeSizingById = sizing.byId
    maxDomain = sizing.maxDomain
    stdNullRadiusSize = sizing.stdNullRadiusSize
    nodeMargins = getNodeMargins()
  }

  function forceLinkDistance(link) {
    let baseDistance =
      (nodeMargins.radius_size[link.source.id] ?? linkNodeSize) +
      (nodeMargins.radius_size[link.target.id] ?? linkNodeSize) +
      Math.max(maxRadiusSize, addedMarginSize, 15)

    return nodes.length == 2 ? Math.max(baseDistance, dyadMinLinkDistance) : baseDistance
  }

  function createLegacySimulation() {
    stopSimulation()

    if (!nodes.length || !width || !height) return

    let nodeById = new Map(nodes.map(node => [node.id, node]))
    primaryNode = identifyPrimaryNode()
    let hasPrimaryNode = primaryNode != null

    linkNodes = links
      .map(link => ({
        source: nodeById.get(linkEndpointId(link, "source")),
        target: nodeById.get(linkEndpointId(link, "target"))
      }))
      .filter(linkNode => linkNode.source && linkNode.target)

    let averageNodeRadius = averageValue(Object.values(nodeMargins.radius_size))
    let averageHorizontalNameShift = averageValue(Object.values(nodeMargins.horizontal_name_shift))

    simulation = forceSimulation(nodes.concat(linkNodes))
      .force("charge", forceManyBody().strength(hasPrimaryNode ? -7500 : -1000))
      .force("center", forceCenter(graphCenterX, graphCenterY))
      .force("x", forceX(graphCenterX).strength(hasPrimaryNode ? 0.75 : 0.15))
      .force("y", forceY(height - addedMarginSize * 2).strength(hasPrimaryNode ? 0.75 : 0.5))
      .force(
        "collision",
        forceCollide()
          .radius(d => {
            if (d.source !== undefined) return Math.max(linkNodeSize, addedMarginSize)

            return Math.max(
              (nodeMargins.radius_size[d.id] ?? 0) +
                Math.abs(nodeMargins.horizontal_name_shift[d.id] ?? 0) +
                Math.abs(nodeMargins.vertical_name_shift[d.id] ?? 0) +
                addedMarginSize,
              averageNodeRadius + averageHorizontalNameShift + addedMarginSize,
              addedMarginSize
            )
          })
          .strength(1)
      )
      .force(
        "link",
        forceLink(links)
          .id(d => Number.parseInt(d.id))
          .distance(forceLinkDistance)
          .strength(0.75)
      )
      .on("tick", () => {
        linkNodes.forEach(linkNode => {
          linkNode.x =
            (getXAdjusted(linkNode.source.id, linkNode.source.x) +
              getXAdjusted(linkNode.target.id, linkNode.target.x)) *
            0.5
          linkNode.y =
            (getYAdjusted(linkNode.source.id, linkNode.source.y) +
              getYAdjusted(linkNode.target.id, linkNode.target.y)) *
            0.5
        })
        refreshGraph()
      })
  }

  function graphPoint(event) {
    let rect = svg.getBoundingClientRect()

    return {
      x: ((event.clientX - rect.left) / rect.width) * width,
      y: ((event.clientY - rect.top) / rect.height) * height
    }
  }

  function tooltipBounds() {
    let rect = svg.parentElement.getBoundingClientRect()

    return {
      width: rect.width || width,
      height: rect.height || height
    }
  }

  function clampTooltipCoordinates(x, y, bounds) {
    return {
      x: Math.max(tooltipPadding, Math.min(bounds.width - tooltipWidth - tooltipPadding, x)),
      y: Math.max(tooltipPadding, Math.min(bounds.height - tooltipHeight - tooltipPadding, y))
    }
  }

  function tooltipPoint(event) {
    let rect = svg.parentElement.getBoundingClientRect()
    let x = event.clientX - rect.left + tooltipOffset
    let y = event.clientY - rect.top + tooltipOffset

    return clampTooltipCoordinates(x, y, { width: rect.width || width, height: rect.height || height })
  }

  function showTooltip(node, event) {
    if (dragNode) return

    hoverNode = node
    tooltip = { node, ...tooltipPoint(event) }
  }

  function clearTooltip() {
    if (!dragNode) {
      hoverNode = null
      tooltip = null
    }
  }

  function moveTooltip(event) {
    if (hoverNode && !dragNode) {
      tooltip = { node: hoverNode, ...tooltipPoint(event) }
    }
  }

  function startDrag(node, event) {
    event.preventDefault()
    event.stopPropagation()
    dragNode = node
    hoverNode = null
    tooltip = null
    let point = graphPoint(event)
    let adjustedPoint = { x: getXAdjusted(node.id, point.x), y: getYAdjusted(node.id, point.y) }
    node.fx = adjustedPoint.x
    node.fy = adjustedPoint.y
    node.x = adjustedPoint.x
    node.y = adjustedPoint.y

    if (simulation) {
      simulation.alphaTarget(0.3).restart()
    }
  }

  function drag(event) {
    if (!dragNode) return

    let point = graphPoint(event)
    let adjustedPoint = { x: getXAdjusted(dragNode.id, point.x), y: getYAdjusted(dragNode.id, point.y) }
    dragNode.fx = adjustedPoint.x
    dragNode.fy = adjustedPoint.y
    dragNode.x = adjustedPoint.x
    dragNode.y = adjustedPoint.y
    refreshGraph()
  }

  function endDrag() {
    if (!dragNode) return

    dragNode.fx = null
    dragNode.fy = null
    dragNode = null
    hoverNode = null
    tooltip = null

    if (simulation) {
      simulation.alphaTarget(0)
    }
  }

  function resetSimulation() {
    stopSimulation()

    descriptorNodes = enrichedNodes(graph?.nodes || [])
    descriptorLinks = (graph?.links || []).map(d => ({ ...d }))
    nodes = descriptorNodes.map(d => ({ ...d }))
    links = descriptorLinks.map(d => ({ ...d }))
    linkNodes = []
    tooltip = null
    hoverNode = null
    dragNode = null
    selectValue.timeframe = timeframeItems[2]
    currentSizingSignature = null
    currentLinkDescriptorSignature = null
  }

  function resetLinkDashStrokeWidth() {
    linkDashStrokeWidth = 3
  }

  function triggerLinkDashPulse() {
    clearTimeout(linkDashPulseTimer)
    linkDashStrokeWidth = 7
    linkDashPulseTimer = setTimeout(resetLinkDashStrokeWidth, 1050)
  }

  function updateLinkDescriptorValue(event) {
    selectValue.linkDescriptor = event.detail.d
    triggerLinkDashPulse()
    refreshGraph()
  }

  function hasTimeframeDescriptor(row, timeframe) {
    return Object.values(row[timeframe] || {}).some(value => numberValue(value) != null)
  }

  function hasNodeTimeframeData(node, timeframe) {
    return (
      hasTimeframeDescriptor(node, timeframe) ||
      Object.values(node.metrics?.[timeframe] || {}).some(value => numberValue(value) != null)
    )
  }

  $: if (graph !== currentGraph || width !== currentWidth) {
    currentGraph = graph
    currentWidth = width
    resetSimulation()
  }

  $: {
    let nextTimeframeItems = timeframeItems.filter(
      item =>
        descriptorNodes.some(node => hasNodeTimeframeData(node, item.value)) ||
        descriptorLinks.some(link => hasTimeframeDescriptor(link, item.value))
    )

    selectItems.timeframe = nextTimeframeItems

    if (!nextTimeframeItems.length) {
      selectValue.timeframe = null
    } else if (nextTimeframeItems.length == 1 && selectValue.timeframe?.value != nextTimeframeItems[0].value) {
      selectValue.timeframe = nextTimeframeItems[0]
    } else if (!nextTimeframeItems.some(item => item.value == selectValue.timeframe?.value)) {
      selectValue.timeframe = nextTimeframeItems[nextTimeframeItems.length - 1]
    }
  }
  $: {
    let nextNodeDescriptorItems = nodeFieldItems(descriptorNodes, selectValue.timeframe?.value || "all_years")
    let nextLinkDescriptorItems = linkFieldItems(descriptorLinks, selectValue.timeframe?.value || "all_years")

    selectItems.nodeDescriptor = nextNodeDescriptorItems
    selectItems.linkDescriptor = nextLinkDescriptorItems
    syncSelectValue("nodeDescriptor", nextNodeDescriptorItems)
    syncSelectValue("linkDescriptor", nextLinkDescriptorItems)
  }
  $: timeframePlaceholder = !selectItems.timeframe.length && !selectValue.timeframe ? noTimeframeItemsMessage : ""
  $: nodeDescriptorPlaceholder =
    !selectItems.nodeDescriptor.length && !selectValue.nodeDescriptor ? noNodeSizeItemsMessage : "Choose a node size."
  $: linkDescriptorPlaceholder =
    !selectItems.linkDescriptor.length && !selectValue.linkDescriptor ? noLinkDashItemsMessage : "Choose a link dash."
  $: nodeDescriptorTooltip = selectValue.nodeDescriptor?.value
    ? metricTooltip(selectValue.nodeDescriptor.value, controlTooltips.node_size)
    : controlTooltips.node_size
  $: linkDescriptorTooltip = selectValue.linkDescriptor?.value
    ? metricTooltip(selectValue.linkDescriptor.value, controlTooltips.link_dash)
    : controlTooltips.link_dash
  $: sizingSignature = `${selectValue.timeframe?.value || "all_years"}|${selectValue.nodeDescriptor?.value || "none"}|${width}|${height}|${nodes.length}|${links.length}`
  $: if (nodes.length && sizingSignature != currentSizingSignature) {
    currentSizingSignature = sizingSignature
    applyLegacySizing()
    createLegacySimulation()
  }
  $: linkDescriptorSignature = `${selectValue.timeframe?.value || "all_years"}|${selectValue.linkDescriptor?.value || "none"}`
  $: if (linkDescriptorSignature != currentLinkDescriptorSignature) {
    currentLinkDescriptorSignature = linkDescriptorSignature
    triggerLinkDashPulse()
  }
  $: hasNodeSizeSignal = hasPositiveFiniteValue(nodeSizingValues)
  $: unknownNodeSizeCount = Object.values(nodeSizingById).filter(sizing => sizing?.isUnknown).length
  $: showNodeSizeWarnings = Boolean(
    selectValue.nodeDescriptor?.value &&
    hasNodeSizeSignal &&
    unknownNodeSizeCount > 0 &&
    unknownNodeSizeCount <= maxVisibleNodeSizeWarnings
  )
  $: if (tooltip) {
    let point = clampTooltipCoordinates(tooltip.x, tooltip.y, tooltipBounds())

    if (point.x != tooltip.x || point.y != tooltip.y) {
      tooltip = { ...tooltip, ...point }
    }
  }

  onDestroy(() => {
    stopSimulation()
    clearTimeout(linkDashPulseTimer)
  })
</script>

<svelte:window on:pointermove={drag} on:pointerup={endDrag} />
<main
  class="relative flex h-full w-full flex-col items-center justify-center"
  bind:clientWidth={pageWidth}
  data-svelte-lib-tooltip-root
>
  <div class="px-8 py-12 text-center text-lg min-[1300px]:hidden">
    This visualization is best viewed on a larger screen. So, grab a computer and come back soon!
  </div>
  <div class="hidden min-[1300px]:block" style="width:{pageWidth ? pageWidth * 0.7 : 0}px">
    <div class="mx-auto flex w-full flex-col gap-4 py-5">
      <section class="grid gap-3 border border-[#d8d3c4] bg-white p-4">
        <div>
          <div class="mb-2 text-sm font-extrabold text-[#596b64]">War Types</div>
          <div class="flex flex-wrap gap-x-5 gap-y-2 text-sm">
            {#each warTypeItems as warTypeItem (warTypeItem.value)}
              <CheckboxFilter
                labelClasses="mb-0 font-medium"
                label={warTypeItem.label}
                value={warTypeItem.value}
                selection={selectedWarTypes}
                deselection={deselectedWarTypes}
                on:update={({ detail }) => {
                  selectedWarTypes = detail.selection
                  deselectedWarTypes = detail.deselection
                }}
              />
            {/each}
          </div>
        </div>
        <div>
          <div class="mb-1 flex items-center gap-2 text-sm font-extrabold text-[#596b64]">
            Country
            <InfoIcon title={controlTooltips.country} tooltipClasses="max-w-80" />
          </div>
          <Select
            items={selectItems.country}
            value={selectValue.country}
            labelConstruction={true}
            secondaryLabelIdentifier="secondaryLabel"
            placeholder="Filter by country"
            noItemsMessage="No countries available."
            on:valueChange={({ detail: e }) => (selectValue.country = e.d)}
          />
        </div>
        <div>
          <div class="mb-1 flex items-center gap-2 text-sm font-extrabold text-[#596b64]">
            War
            <InfoIcon title={controlTooltips.war} tooltipClasses="max-w-80" />
          </div>
          <Select
            items={selectItems.war}
            value={selectValue.war}
            groupBy="war_type"
            labelConstruction={true}
            secondaryLabelIdentifier="secondaryLabel"
            placeholder="Select a war"
            noItemsMessage="No wars match the selected filters."
            on:valueChange={({ detail: e }) => (selectValue.war = e.d)}
          />
        </div>
      </section>
      <div class="relative w-full overflow-hidden border border-solid border-black">
        <section class="min-h-[690px] bg-[#fbfcf9]" bind:clientWidth={width}>
          <div class="flex flex-col gap-4 border-b border-[#d2d7d3] bg-white px-4 py-3">
            {#if selectedWar}
              <div class="grid gap-3 text-sm md:grid-cols-3 md:items-start">
                <div class="grid gap-1 font-semibold">
                  <div>
                    <span class="font-bold text-[#60706a]">Type:</span>
                    {selectedWar.war_type}
                  </div>
                  <div>
                    <span class="font-bold text-[#60706a]">Subtype:</span>
                    {selectedWar.war_subtype || "Unspecified"}
                  </div>
                </div>
                <div class="self-center text-center">
                  <div class="text-base font-extrabold">{selectedWar.war_name}</div>
                  <div class="mt-1 font-semibold text-[#60706a]">
                    {selectedWar.ongoing_war
                      ? `${selectedWar.start_year}-Present`
                      : selectedWar.start_year == selectedWar.end_year
                        ? String(selectedWar.start_year)
                        : `${selectedWar.start_year}-${selectedWar.end_year}`}
                    ({Number(selectedWar.total_days_in_war || 0).toLocaleString()} days)
                  </div>
                </div>
                <div aria-hidden="true"></div>
              </div>
            {/if}
            {#if nodes.length}
              <div class="grid gap-3 md:grid-cols-3">
                <div>
                  <div class="mb-1 flex items-center gap-2 text-sm font-extrabold text-[#596b64]">
                    Timeframe
                    <InfoIcon title={controlTooltips.timeframe} tooltipClasses="max-w-80" />
                  </div>
                  <Select
                    items={selectItems.timeframe}
                    value={selectValue.timeframe}
                    placeholder={timeframePlaceholder}
                    noItemsMessage={noTimeframeItemsMessage}
                    clearable={false}
                    disabled={selectItems.timeframe.length <= 1}
                    on:valueChange={({ detail: e }) => (selectValue.timeframe = e.d)}
                  />
                </div>
                <div>
                  <div class="mb-1 flex items-center gap-2 text-sm font-extrabold text-[#596b64]">
                    Node Size
                    <InfoIcon title={nodeDescriptorTooltip} tooltipClasses="max-w-96" />
                  </div>
                  <Select
                    items={selectItems.nodeDescriptor}
                    value={selectValue.nodeDescriptor}
                    placeholder={nodeDescriptorPlaceholder}
                    noItemsMessage={noNodeSizeItemsMessage}
                    clearable={true}
                    disabled={!selectItems.nodeDescriptor.length && !selectValue.nodeDescriptor}
                    on:valueChange={({ detail: e }) => (selectValue.nodeDescriptor = e.d)}
                  />
                </div>
                <div>
                  <div class="mb-1 flex items-center gap-2 text-sm font-extrabold text-[#596b64]">
                    Link Dash
                    <InfoIcon title={linkDescriptorTooltip} tooltipClasses="max-w-96" />
                  </div>
                  <Select
                    items={selectItems.linkDescriptor}
                    value={selectValue.linkDescriptor}
                    placeholder={linkDescriptorPlaceholder}
                    noItemsMessage={noLinkDashItemsMessage}
                    clearable={true}
                    disabled={!selectItems.linkDescriptor.length && !selectValue.linkDescriptor}
                    on:valueChange={updateLinkDescriptorValue}
                  />
                </div>
              </div>
            {/if}
          </div>
          {#if nodes.length}
            <div class="relative">
              <svg
                class="block w-full touch-none"
                {height}
                viewBox="0 0 {width} {height}"
                role="img"
                bind:this={svg}
                on:pointermove={moveTooltip}
                on:pointerleave={clearTooltip}
              >
                <g>
                  {#each links as link, i (i)}
                    <line
                      x1={linkX(link, "source")}
                      y1={linkY(link, "source")}
                      x2={linkX(link, "target")}
                      y2={linkY(link, "target")}
                      stroke="#8a948f"
                      stroke-opacity={0.45}
                      stroke-width={1}
                    />
                    <line
                      x1={linkX(link, "source")}
                      y1={linkY(link, "source")}
                      x2={linkX(link, "target")}
                      y2={linkY(link, "target")}
                      stroke={linkHasDescriptor(link) ? "blue" : "transparent"}
                      stroke-opacity={0.9}
                      stroke-width={linkDashStrokeWidth}
                      stroke-dasharray="2.5 15"
                      stroke-dashoffset={-7.5}
                      style="transition: stroke-width 1000ms ease 50ms, stroke 1000ms ease 50ms;"
                    />
                  {/each}
                </g>
                <g role="list">
                  {#each nodes as node (node.id)}
                    {@const label = labelPosition(node)}
                    <g
                      class="cursor-grab active:cursor-grabbing"
                      role="listitem"
                      transform="translate({getXAdjusted(node.id, node.x)}, {getYAdjusted(node.id, node.y)})"
                      on:pointerdown={event => startDrag(node, event)}
                      on:pointerenter={event => showTooltip(node, event)}
                      on:pointermove={event => showTooltip(node, event)}
                      on:pointerleave={clearTooltip}
                    >
                      <circle
                        r={nodeRadius(node)}
                        fill={sideColors[node.side]}
                        stroke="black"
                        stroke-width={hoverNode?.id == node.id ? nodeStrokeWidth + 0.75 : nodeStrokeWidth}
                        style="transition: r 3000ms ease 500ms, stroke-width 150ms ease;"
                      />
                      <g
                        style="transform: translate({label.x}px, {label.y}px); transition: transform 2000ms ease 1500ms;"
                      >
                        <text
                          class="text-[12px] font-bold"
                          text-anchor={label.anchor}
                          fill={label.inside ? "white" : "#111827"}
                          stroke={label.inside ? "none" : "white"}
                          stroke-width={label.inside ? 0 : 3}
                          paint-order="stroke"
                          style="transition: fill 2000ms ease 1500ms, stroke 2000ms ease 1500ms;"
                        >
                          {node.participant}
                        </text>
                      </g>
                      {#if showNodeSizeWarning(node)}
                        {@const warningPosition = nodeSizeWarningPosition(node, label)}
                        <g
                          style="transform: translate({warningPosition.x}px, {warningPosition.y}px); transition: transform 2000ms ease 1500ms;"
                        >
                          <text
                            class="text-[10px] font-extrabold"
                            text-anchor="middle"
                            dominant-baseline="central"
                            fill="#111827"
                            stroke="white"
                            stroke-width={2.5}
                            paint-order="stroke"
                          >
                            ?
                          </text>
                        </g>
                      {/if}
                    </g>
                  {/each}
                </g>
              </svg>
              {#if tooltip}
                {@const metricRows = nodeMetricRows(tooltip.node)}
                <div
                  class="pointer-events-none absolute z-20 max-w-sm border border-[#c4cec8] bg-white px-3 py-2 text-xs shadow-sm"
                  style="left: {tooltip.x}px; top: {tooltip.y}px;"
                  bind:clientWidth={tooltipWidth}
                  bind:clientHeight={tooltipHeight}
                >
                  <div class="text-sm font-extrabold">{tooltip.node.participant}</div>
                  <div class="mt-1 space-y-0.5 text-[#50615b]">
                    <div>
                      <span class="font-bold text-[#33413c]">Start Date:</span>
                      {displayDate(tooltip.node.start_date, tooltip.node.start_date_estimated)}
                    </div>
                    <div>
                      <span class="font-bold text-[#33413c]">End Date:</span>
                      {Number(tooltip.node.ongoing_war) == 1
                        ? "Ongoing"
                        : displayDate(tooltip.node.end_date, tooltip.node.end_date_estimated)}
                    </div>
                    <div class="mb-2">
                      <span class="font-bold text-[#33413c]">Days At War:</span>
                      {displayMetricNumber(tooltip.node.metrics?.all_years?.days_at_war, "days_at_war")}
                    </div>
                  </div>
                  {#if metricRows.length}
                    <div class="mt-2 border-t border-[#dfe5e1] pt-1.5">
                      <div class="mb-1 font-extrabold text-[#33413c]">
                        Timeframe: {selectValue.timeframe?.label || "All Years"}
                      </div>
                      <div class="space-y-0.5 text-[#50615b]">
                        {#each metricRows as row (row.field)}
                          <div>
                            <span class="font-semibold text-[#33413c]">{row.label}:</span>
                            {row.value}
                          </div>
                        {/each}
                      </div>
                    </div>
                  {/if}
                </div>
              {/if}
            </div>
          {:else}
            <div class="flex h-[700px] items-center justify-center px-6 text-center text-[#60706a]">
              No graph rows are available for the current selection.
            </div>
          {/if}
        </section>
      </div>
    </div>
  </div>
</main>
