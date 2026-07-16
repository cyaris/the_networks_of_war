<script>
  import { forceCenter, forceCollide, forceLink, forceManyBody, forceSimulation, forceX, forceY } from "d3-force"
  import { scaleLinear } from "d3-scale"
  import { onDestroy } from "svelte"
  import { Select } from "svelte-lib/components"

  import graphData from "../static/graphData.json"

  let wars = graphData.wars
  let graphsByWarId = graphData.graphsByWarId

  const timeframeItems = [
    { value: "first_year", label: "First Year" },
    { value: "last_year", label: "Last Year" },
    { value: "all_years", label: "All Years" },
  ]

  function plural(value, noun) {
    return `${Number(value || 0).toLocaleString()} ${noun}${Number(value || 0) == 1 ? "" : "s"}`
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

  let linkDashFieldCountsByWarId = Object.fromEntries(wars.map(war => [String(war.war_id), linkDashFieldCount(war)]))
  let warTypeItems = Array.from(new Set(wars.map(war => war.war_type)))
    .sort()
    .map(warType => ({
      value: warType,
      label: warType,
    }))
  let selectedWarTypes = [...warTypeItems]
  let selectedWarItem = null
  let graph = { nodes: [], links: [] }
  let selectedGraph = null
  let selectedWar = null
  let pageWidth
  let toolViewportWidth

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
  let nodeDescriptiveValues = []
  let maxDomain = 2
  let stdNullRadiusSize = 1
  let nodeMargins = emptyNodeMargins()
  let radiusScale = scaleLinear([0, maxDomain], [1, 125])
  let primaryNode = null

  const graphTextSize = 12
  const height = 700
  const minRadiusSize = 1
  const maxRadiusSize = 125
  const linkNodeSize = 2.5
  const nodeStrokeWidth = 1
  const nodeSizeWarningOffset = 10
  const graphCenterY = height * 0.5
  const addedMarginSize = Math.max(linkNodeSize, 10)
  const tooltipOffset = 16
  const tooltipPadding = 8

  let timeframeValue = timeframeItems[2]
  let nodeDescriptorValue = null
  let linkDescriptorValue = null

  const sideColors = { 1: "#2f7f66", 2: "#b54f72", 3: "#5f70b8", null: "#71717a", undefined: "#71717a" }

  $: graphCenterX = width * 0.5
  $: selectedWarTypeValues = selectedWarTypes?.length ? selectedWarTypes.map(d => d.value) : []
  $: filteredWars = selectedWarTypeValues.length
    ? wars.filter(war => selectedWarTypeValues.includes(war.war_type))
    : wars
  $: warItems = filteredWars.map(war => ({
    value: String(war.war_id),
    label: war.war_name,
    selectedLabel: war.war_name,
    secondaryLabel: warSecondaryLabel(war),
    war_type: war.war_type,
    linkDashFieldCount: linkDashFieldCountsByWarId[String(war.war_id)] || 0,
    war,
  }))
  $: if (warItems.length && !warItems.some(item => item.value == selectedWarItem?.value)) {
    selectedWarItem = preferredWarItem(warItems)
  }
  $: selectedWar = selectedWarItem?.war
  $: selectedGraph = selectedWar ? graphForWar(selectedWar) : null
  $: graph = selectedGraph || { nodes: [], links: [] }
  $: toolViewportWidth = pageWidth ? pageWidth * 0.7 : 0

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
      added_right_margin: {},
    }
  }

  function numberValue(value) {
    if (value == null || value === "") return null

    let parsed = Number(value)

    return Number.isFinite(parsed) ? parsed : null
  }

  function coalescedUniqueValues(values) {
    return Array.from(new Set(values.map(value => (Number.isFinite(value) ? value : 0))))
  }

  function maxValue(values) {
    return Math.max(...values.map(value => (Number.isFinite(value) ? value : 0)), 0)
  }

  function averageValue(values) {
    let finiteValues = values.filter(Number.isFinite)

    return finiteValues.length ? finiteValues.reduce((total, value) => total + value, 0) / finiteValues.length : 0
  }

  function fieldLabel(field) {
    return field.replaceAll("_", " ").replace(/\b\w/g, value => value.toUpperCase())
  }

  function descriptorFields(rows, timeframe) {
    return Array.from(new Set(rows.flatMap(row => Object.keys(row[timeframe] || {}))))
  }

  function descriptorItems(fields) {
    return fields
      .sort((a, b) => fieldLabel(a).localeCompare(fieldLabel(b)))
      .map(field => ({ value: field, label: fieldLabel(field), secondaryLabel: field }))
  }

  function descriptorValue(row, field, timeframe = timeframeValue?.value || "all_years") {
    return row?.[timeframe]?.[field]
  }

  function getNodeDescriptiveValues(sizeField) {
    let values = []
    let descriptorMaxDomain = 0
    let descriptorNullRadiusNodes = 0

    descriptorNodes.forEach(node => {
      let sizeValue

      if (sizeField == "days_at_war") {
        let totalDays = dateDays(node.start_date, Number(node.ongoing_war) == 1 ? null : node.end_date)
        sizeValue = totalDays == null ? NaN : totalDays - (numberValue(descriptorValue(node, "days_not_at_war")) ?? 0)
      } else if (sizeField == "battle_deaths") {
        sizeValue = numberValue(node.battle_deaths) ?? NaN
      } else if (sizeField == "battle_deaths_per_day") {
        let totalDays = dateDays(node.start_date, Number(node.ongoing_war) == 1 ? null : node.end_date)
        let daysAtWar =
          totalDays == null ? null : totalDays - (numberValue(descriptorValue(node, "days_not_at_war")) ?? 0)
        let battleDeaths = numberValue(descriptorValue(node, "battle_deaths")) ?? numberValue(node.battle_deaths)
        sizeValue =
          Number.isFinite(daysAtWar) && battleDeaths != null ? Math.round((battleDeaths / daysAtWar) * 100) / 100 : NaN
      } else {
        sizeValue = Number.parseFloat(descriptorValue(node, sizeField))
      }

      if (Number.isNaN(sizeValue)) {
        descriptorNullRadiusNodes += 1
        values.push(NaN)
      } else {
        let value = Math.max(0, sizeValue)
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

    return [values, descriptorMaxDomain, descriptorStdNullRadiusSize, descriptorNullRadiusNodes]
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

    let daysAtWarUniqueValues = coalescedUniqueValues(getNodeDescriptiveValues("days_at_war")[0])
    let daysNotAtWarUniqueValues = coalescedUniqueValues(getNodeDescriptiveValues("days_not_at_war")[0])

    return descriptorItems(
      fields.filter(field => {
        let [values, , , fieldNullRadiusNodes] = getNodeDescriptiveValues(field)
        let uniqueValues = coalescedUniqueValues(values)

        if (maxValue(uniqueValues) == 0) return false
        if (uniqueValues.length == 1) return false
        if (fieldNullRadiusNodes / values.length >= 0.5) return false
        if (field == "days_at_war" && daysAtWarUniqueValues.length == 1) return false
        if (field == "days_not_at_war" && daysNotAtWarUniqueValues.length == 1) return false
        if (field == "battle_deaths_per_day" && daysAtWarUniqueValues.length == 1) return false

        return true
      })
    )
  }

  function linkFieldItems(rows, timeframe) {
    return descriptorItems(
      descriptorFields(rows, timeframe).filter(field => maxValue(getLinkDescriptiveValues(field)) == 1)
    )
  }

  function dateDays(startDate, endDate) {
    let start = new Date(`${startDate}T00:00:00`)
    let end = endDate ? new Date(`${endDate}T00:00:00`) : new Date()
    let days = Math.round((end - start) / 86400000) + 1

    return Number.isFinite(days) ? Math.max(1, days) : null
  }

  function enrichedNodes(rawNodes) {
    return rawNodes.map(node => {
      let daysAtWar = dateDays(node.start_date, node.ongoing_war ? null : node.end_date)
      let battleDeaths = numberValue(node.battle_deaths)

      return {
        ...node,
        all_years: {
          ...(node.all_years || {}),
          battle_deaths: battleDeaths,
          days_at_war: daysAtWar,
          battle_deaths_per_day:
            Number.isFinite(daysAtWar) && battleDeaths != null
              ? Math.round((battleDeaths / daysAtWar) * 100) / 100
              : null,
        },
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
      let nodeValue = nodeDescriptiveValues[node.id]
      let nodeHasSelectedDescriptor = Boolean(nodeDescriptorValue?.value)
      let currentRadiusSize = Number.isNaN(Number.parseFloat(nodeValue))
        ? nodeHasSelectedDescriptor
          ? minRadiusSize
          : radiusScale(stdNullRadiusSize)
        : radiusScale(nodeValue)
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
        inside: true,
      }
    }

    return {
      x: (nodeMargins.horizontal_name_shift[id] ?? 30) * xOperator,
      y: (nodeMargins.vertical_name_shift[id] ?? nodeRadius(node) + 22.5) * yOperator + yAdjustment,
      anchor: "middle",
      inside: false,
    }
  }

  function nodeSizeWarningPosition(node, label = labelPosition(node)) {
    let offset = Math.max(nodeMargins.horizontal_name_shift[node.id] ?? 0, nodeRadius(node) + nodeSizeWarningOffset)
    let outsideXOperator = label.x < 0 ? 1 : -1
    let outsideYOperator = label.y < 0 ? 1 : -1

    return nodeMargins.name_fits_in_node[node.id]
      ? { x: offset, y: offset }
      : { x: offset * outsideXOperator, y: offset * outsideYOperator }
  }

  function showNodeSizeWarning(node) {
    if (!nodeDescriptorValue?.value || maxValue(coalescedUniqueValues(nodeDescriptiveValues)) == 0) return false

    return !Number.isFinite(nodeDescriptiveValues[node.id])
  }

  function nodeDescriptorDisplayValue(node) {
    if (!nodeDescriptorValue?.value) return null

    let value =
      nodeDescriptorValue.value == "battle_deaths"
        ? numberValue(node.battle_deaths)
        : numberValue(descriptorValue(node, nodeDescriptorValue.value))

    return value == null ? "Unknown" : value.toLocaleString()
  }

  function displayNumber(value) {
    let parsed = numberValue(value)

    return parsed == null ? "Unknown" : parsed.toLocaleString()
  }

  function linkHasDescriptor(link) {
    return linkDescriptorValue?.value && (numberValue(descriptorValue(link, linkDescriptorValue.value)) ?? 0) > 0
  }

  function applyLegacySizing() {
    ;[nodeDescriptiveValues, maxDomain, stdNullRadiusSize] = getNodeDescriptiveValues(
      nodeDescriptorValue?.value || null
    )
    nodeMargins = getNodeMargins()
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
        target: nodeById.get(linkEndpointId(link, "target")),
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
          .distance(
            d =>
              (nodeMargins.radius_size[d.source.id] ?? linkNodeSize) +
              (nodeMargins.radius_size[d.target.id] ?? linkNodeSize) +
              Math.max(maxRadiusSize, addedMarginSize, 15)
          )
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
        refreshGraphRows()
      })
  }

  function graphPoint(event) {
    let rect = svg.getBoundingClientRect()

    return {
      x: ((event.clientX - rect.left) / rect.width) * width,
      y: ((event.clientY - rect.top) / rect.height) * height,
    }
  }

  function tooltipBounds() {
    let rect = svg.parentElement.getBoundingClientRect()

    return {
      width: rect.width || width,
      height: rect.height || height,
    }
  }

  function clampTooltipCoordinates(x, y) {
    let bounds = tooltipBounds()

    return {
      x: Math.max(tooltipPadding, Math.min(bounds.width - tooltipWidth - tooltipPadding, x)),
      y: Math.max(tooltipPadding, Math.min(bounds.height - tooltipHeight - tooltipPadding, y)),
    }
  }

  function tooltipPoint(event) {
    let rect = svg.parentElement.getBoundingClientRect()
    let x = event.clientX - rect.left + tooltipOffset
    let y = event.clientY - rect.top + tooltipOffset

    return clampTooltipCoordinates(x, y)
  }

  function showTooltip(node, event) {
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

  function refreshGraphRows() {
    nodes = nodes
    refreshLinks()
  }

  function refreshLinks() {
    links = links
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
    refreshGraphRows()
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
    timeframeValue = timeframeItems[2]
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
    linkDescriptorValue = event.detail.d
    triggerLinkDashPulse()
    refreshLinks()
  }

  function stopSimulation() {
    if (simulation) {
      simulation.stop()
    }
  }

  function hasTimeframeDescriptor(row, timeframe) {
    return Object.values(row[timeframe] || {}).some(value => numberValue(value) != null)
  }

  $: if (graph !== currentGraph || width !== currentWidth) {
    currentGraph = graph
    currentWidth = width
    resetSimulation()
  }

  $: availableTimeframeItems = timeframeItems.filter(
    item =>
      item.value == "z" ||
      descriptorNodes.some(node => hasTimeframeDescriptor(node, item.value)) ||
      descriptorLinks.some(link => hasTimeframeDescriptor(link, item.value))
  )
  $: if (!availableTimeframeItems.some(item => item.value == timeframeValue?.value)) {
    timeframeValue = availableTimeframeItems[availableTimeframeItems.length - 1] || timeframeItems[2]
  }
  $: nodeDescriptorItems = nodeFieldItems(descriptorNodes, timeframeValue?.value || "all_years")
  $: linkDescriptorItems = linkFieldItems(descriptorLinks, timeframeValue?.value || "all_years")
  $: if (nodeDescriptorValue && !nodeDescriptorItems.some(item => item.value == nodeDescriptorValue.value)) {
    nodeDescriptorValue = null
  }
  $: if (linkDescriptorValue && !linkDescriptorItems.some(item => item.value == linkDescriptorValue.value)) {
    linkDescriptorValue = null
  }
  $: sizingSignature = `${timeframeValue?.value || "all_years"}|${nodeDescriptorValue?.value || "none"}|${width}|${height}|${nodes.length}|${links.length}`
  $: if (nodes.length && sizingSignature != currentSizingSignature) {
    currentSizingSignature = sizingSignature
    applyLegacySizing()
    createLegacySimulation()
  }
  $: linkDescriptorSignature = `${timeframeValue?.value || "all_years"}|${linkDescriptorValue?.value || "none"}`
  $: if (linkDescriptorSignature != currentLinkDescriptorSignature) {
    currentLinkDescriptorSignature = linkDescriptorSignature
    triggerLinkDashPulse()
  }
  $: if (tooltip) {
    let point = clampTooltipCoordinates(tooltip.x, tooltip.y)

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
<main class="flex h-full w-full flex-col items-center justify-center" bind:clientWidth={pageWidth}>
  <div class="px-8 py-12 text-center text-lg min-[1300px]:hidden">
    This visualization is best viewed on a larger screen. So, grab a computer and come back soon!
  </div>
  <div class="hidden min-[1300px]:block" style="width:{toolViewportWidth}px">
    <div class="mx-auto flex w-full flex-col gap-4 py-5">
      <section class="grid gap-3 border border-[#d8d3c4] bg-white p-4">
        <div>
          <div class="mb-1 text-sm font-extrabold text-[#596b64]">War Types</div>
          <Select
            items={warTypeItems}
            bind:value={selectedWarTypes}
            multiple={true}
            placeholder="Filter war types"
            on:valueChange={({ detail }) => (selectedWarTypes = detail.d || [])}
          />
        </div>
        <div>
          <div class="mb-1 text-sm font-extrabold text-[#596b64]">War</div>
          <Select
            items={warItems}
            bind:value={selectedWarItem}
            groupBy="war_type"
            labelConstruction={true}
            secondaryLabelIdentifier="secondaryLabel"
            placeholder="Select a war"
            noItemsMessage="No wars match the selected filters."
            on:valueChange={({ detail }) => (selectedWarItem = detail.d)}
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
                  <div class="mb-1 text-sm font-extrabold text-[#596b64]">Timeframe</div>
                  <Select
                    items={availableTimeframeItems}
                    bind:value={timeframeValue}
                    clearable={false}
                    disabled={availableTimeframeItems.length <= 1}
                  />
                </div>
                <div>
                  <div class="mb-1 text-sm font-extrabold text-[#596b64]">Node Size</div>
                  <Select
                    items={nodeDescriptorItems}
                    bind:value={nodeDescriptorValue}
                    placeholder="Choose a node size."
                    noItemsMessage="No node size fields available."
                    clearable={true}
                  />
                </div>
                <div>
                  <div class="mb-1 text-sm font-extrabold text-[#596b64]">Link Dash</div>
                  <Select
                    items={linkDescriptorItems}
                    bind:value={linkDescriptorValue}
                    placeholder="Choose a link dash."
                    noItemsMessage="No link dash fields available."
                    clearable={true}
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
                            class="text-[12px] font-bold"
                            text-anchor="middle"
                            fill="#111827"
                            stroke="white"
                            stroke-width={3}
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
                <div
                  class="pointer-events-none absolute z-20 max-w-xs border border-[#c4cec8] bg-white px-3 py-2 text-sm shadow-sm"
                  style="left: {tooltip.x}px; top: {tooltip.y}px;"
                  bind:clientWidth={tooltipWidth}
                  bind:clientHeight={tooltipHeight}
                >
                  <div class="font-extrabold">{tooltip.node.participant}</div>
                  <div class="text-[#50615b]">
                    Battle Deaths: {displayNumber(tooltip.node.battle_deaths)}
                  </div>
                  {#if nodeDescriptorValue?.value && nodeDescriptorValue.value != "battle_deaths"}
                    <div class="text-[#50615b]">
                      {nodeDescriptorValue.label}: {nodeDescriptorDisplayValue(tooltip.node)}
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
