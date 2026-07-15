<script>
  import { forceCenter, forceCollide, forceLink, forceManyBody, forceSimulation, forceX, forceY } from "d3-force"
  import { scaleLinear } from "d3-scale"
  import { onDestroy } from "svelte"
  import { Select } from "svelte-lib/components"

  export let graph = { nodes: [], links: [] }
  export let selectedWar = null

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

  const graphTextSize = 12
  const height = 700
  const minRadiusSize = 1
  const maxRadiusSize = 125
  const linkNodeSize = 2.5
  const nodeStrokeWidth = 1
  const nodeSizeWarningOffset = 10
  const graphMenuHeight = height * 0.175
  const graphHeight = height - graphMenuHeight
  const graphCenterY = graphMenuHeight + (height - graphMenuHeight) / 2
  const addedMarginSize = Math.max(linkNodeSize, 10)

  const timeframeItems = [
    { value: "x", label: "First Year" },
    { value: "y", label: "Last Year" },
    { value: "z", label: "All Years" },
  ]
  let timeframeValue = timeframeItems[2]
  let nodeDescriptorValue = null
  let linkDescriptorValue = null

  const sideColors = {
    1: "#2f7f66",
    2: "#b54f72",
    3: "#5f70b8",
    null: "#71717a",
    undefined: "#71717a",
  }

  $: graphCenterX = width * 0.5

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
    return field
      .replace(/_[xyz]$/, "")
      .replaceAll("_", " ")
      .replace(/\b\w/g, value => value.toUpperCase())
  }

  function getNodeDescriptiveValues(sizeField) {
    let values = []
    let descriptorMaxDomain = 0
    let descriptorNullRadiusNodes = 0

    descriptorNodes.forEach(node => {
      let sizeValue

      if (sizeField == "days_at_war_z") {
        let totalDays = dateDays(node.start_date, Number(node.ongoing_conflict) == 1 ? null : node.end_date)
        sizeValue = totalDays == null ? NaN : totalDays - (numberValue(node.days_not_at_war_z) ?? 0)
      } else if (sizeField == "battle_deaths_z") {
        sizeValue = numberValue(node.battle_deaths) ?? NaN
      } else if (sizeField == "battle_deaths_per_day_z") {
        let totalDays = dateDays(node.start_date, Number(node.ongoing_conflict) == 1 ? null : node.end_date)
        let daysAtWar = totalDays == null ? null : totalDays - (numberValue(node.days_not_at_war_z) ?? 0)
        let battleDeaths = numberValue(node.battle_deaths_z) ?? numberValue(node.battle_deaths)
        sizeValue =
          Number.isFinite(daysAtWar) && battleDeaths != null ? Math.round((battleDeaths / daysAtWar) * 100) / 100 : NaN
      } else {
        sizeValue = Number.parseFloat(node[sizeField])
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
      let value = Number.parseFloat(link[linkDescriptorName])

      return Number.isFinite(value) && value > 0 ? 1 : 0
    })
  }

  function nodeFieldItems(rows, suffix) {
    let fields = Array.from(
      new Set((rows[0] ? Object.keys(rows[0]) : []).filter(field => field.endsWith(`_${suffix}`)))
    )

    if (suffix == "z") {
      fields = Array.from(new Set([...fields, "days_at_war_z", "battle_deaths_z", "battle_deaths_per_day_z"]))
    }

    let daysAtWarValues = getNodeDescriptiveValues("days_at_war_z")[0]
    let daysNotAtWarValues = getNodeDescriptiveValues("days_not_at_war_z")[0]
    let maxLandMassGain = maxValue(coalescedUniqueValues(getNodeDescriptiveValues("land_mass_exchange_gain_z")[0]))
    let maxLandMassLoss = maxValue(coalescedUniqueValues(getNodeDescriptiveValues("land_mass_exchange_loss_z")[0]))
    let sumAlliedCountries = maxValue(getNodeDescriptiveValues("allied_countries_z")[0])
    let sumTerrorismDeaths = maxValue(getNodeDescriptiveValues("terrorism_deaths_z")[0])
    let sumConcurrentWars = maxValue(getNodeDescriptiveValues("concurrent_wars_z")[0])
    let sumTradeCountries = maxValue(getNodeDescriptiveValues("trade_countries_z")[0])
    let sumMidDyads = maxValue(getNodeDescriptiveValues("mid_dyads_z")[0])
    let sumMidDyadsInitiated = maxValue(getNodeDescriptiveValues("mid_dyads_initiated_z")[0])
    let sumMidDyadsJoined = maxValue(getNodeDescriptiveValues("mid_dyads_joined_z")[0])
    let sumMidDyadsTargeted = maxValue(getNodeDescriptiveValues("mid_dyads_targeted_z")[0])

    let items = fields
      .filter(field => {
        let [values, , , fieldNullRadiusNodes] = getNodeDescriptiveValues(field)
        let uniqueValues = coalescedUniqueValues(values)

        if (maxValue(uniqueValues) == 0) return false
        if (fieldNullRadiusNodes / values.length >= 0.5) return false
        if (field == "days_at_war_z" && coalescedUniqueValues(daysAtWarValues).length == 1) return false
        if (field == "days_not_at_war_z" && coalescedUniqueValues(daysNotAtWarValues).length == 1) return false
        if (
          [
            "land_mass_exchange_gain_x",
            "land_mass_exchange_gain_y",
            "land_mass_exchange_gain_z",
            "population_exchange_gain_x",
            "population_exchange_gain_y",
            "population_exchange_gain_z",
          ].includes(field) &&
          maxLandMassLoss == 0
        )
          return false
        if (
          [
            "land_mass_exchange_loss_x",
            "land_mass_exchange_loss_y",
            "land_mass_exchange_loss_z",
            "population_exchange_loss_x",
            "population_exchange_loss_y",
            "population_exchange_loss_z",
          ].includes(field) &&
          maxLandMassGain == 0
        )
          return false
        if (["concurrent_wars_x", "concurrent_wars_y", "concurrent_wars_z"].includes(field) && sumConcurrentWars <= 3)
          return false
        if (
          ["allied_countries_x", "allied_countries_y", "allied_countries_z"].includes(field) &&
          sumAlliedCountries / 2 <= 3
        )
          return false
        if (
          ["terrorism_deaths_x", "terrorism_deaths_y", "terrorism_deaths_z"].includes(field) &&
          sumTerrorismDeaths <= 100
        )
          return false
        if (
          ["trade_countries_x", "trade_countries_y", "trade_countries_z"].includes(field) &&
          sumTradeCountries / 2 <= 10
        )
          return false
        if (
          [
            "mid_dyads_x",
            "mid_dyads_initiated_x",
            "mid_dyads_joined_x",
            "mid_dyads_targeted_x",
            "mid_dyads_y",
            "mid_dyads_initiated_y",
            "mid_dyads_joined_y",
            "mid_dyads_targeted_y",
            "mid_dyads_z",
            "mid_dyads_initiated_z",
            "mid_dyads_joined_z",
            "mid_dyads_targeted_z",
          ].includes(field) &&
          sumMidDyads / 2 <= descriptorLinks.length
        )
          return false
        if (
          ["mid_dyads_initiated_x", "mid_dyads_initiated_y", "mid_dyads_initiated_z"].includes(field) &&
          sumMidDyadsInitiated / 2 <= descriptorLinks.length + 1
        )
          return false
        if (
          ["mid_dyads_joined_x", "mid_dyads_joined_y", "mid_dyads_joined_z"].includes(field) &&
          sumMidDyadsJoined / 2 <= descriptorLinks.length + 1
        )
          return false
        if (
          ["mid_dyads_targeted_x", "mid_dyads_targeted_y", "mid_dyads_targeted_z"].includes(field) &&
          sumMidDyadsTargeted / 2 <= descriptorLinks.length + 1
        )
          return false
        if (field == "battle_deaths_per_day_z" && coalescedUniqueValues(daysAtWarValues).length == 1) return false

        return true
      })
      .sort((a, b) => fieldLabel(a).localeCompare(fieldLabel(b)))
      .map(field => ({ value: field, label: fieldLabel(field), secondaryLabel: field }))

    return items.length
      ? [{ value: null, label: "None Selected" }, ...items]
      : [{ value: null, label: "None Available", selectable: false }]
  }

  function linkFieldItems(rows, suffix) {
    let items = Array.from(new Set((rows[0] ? Object.keys(rows[0]) : []).filter(field => field.endsWith(`_${suffix}`))))
      .filter(field => maxValue(getLinkDescriptiveValues(field)) == 1)
      .sort((a, b) => fieldLabel(a).localeCompare(fieldLabel(b)))
      .map(field => ({ value: field, label: fieldLabel(field), secondaryLabel: field }))

    return items.length
      ? [{ value: null, label: "None Selected" }, ...items]
      : [{ value: null, label: "None Available", selectable: false }]
  }

  function dateDays(startDate, endDate) {
    let start = new Date(`${startDate}T00:00:00`)
    let end = endDate ? new Date(`${endDate}T00:00:00`) : new Date()
    let days = Math.round((end - start) / 86400000) + 1

    return Number.isFinite(days) ? Math.max(1, days) : null
  }

  function enrichedNodes(rawNodes) {
    return rawNodes.map(node => {
      let daysAtWar = dateDays(node.start_date, node.ongoing_conflict ? null : node.end_date)
      let battleDeaths = numberValue(node.battle_deaths)

      return {
        ...node,
        days_at_war_z: daysAtWar,
        battle_deaths_per_day_z:
          Number.isFinite(daysAtWar) && battleDeaths != null
            ? Math.round((battleDeaths / daysAtWar) * 100) / 100
            : null,
      }
    })
  }

  function textWidth(value) {
    return String(value || "").length * 6.5
  }

  function nameFitsInNode(nodeRadiusValue, nameInput) {
    return nodeRadiusValue * 2 > textWidth(nameInput) + 15
  }

  function verticalNameShift(nodeRadiusValue, participantName) {
    return nameFitsInNode(nodeRadiusValue, participantName) ? 5 : nodeRadiusValue + 22.5
  }

  function horizontalNameShift(nodeRadiusValue, participantName) {
    return nameFitsInNode(nodeRadiusValue, participantName) ? 0 : 30
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
      let currentNameLength = textWidth(node.participant)
      let currentNameLengthHalf = currentNameLength / 2
      let currentVerticalShift = verticalNameShift(currentRadiusSize, node.participant)
      let currentHorizontalShift = horizontalNameShift(currentRadiusSize, node.participant)
      let nameFits = nameFitsInNode(currentRadiusSize, node.participant)

      margins.radius_size[node.id] = currentRadiusSize
      margins.name[node.id] = node.participant
      margins.name_lengths[node.id] = currentNameLength
      margins.vertical_name_shift[node.id] = currentVerticalShift
      margins.horizontal_name_shift[node.id] = currentHorizontalShift
      margins.name_fits_in_node[node.id] = nameFits

      if (!nameFits) {
        margins.added_top_margin[node.id] = height - graphHeight - 2 + currentVerticalShift + addedMarginSize
        margins.added_bottom_margin[node.id] = currentVerticalShift + addedMarginSize
        margins.added_left_margin[node.id] = currentNameLengthHalf + currentHorizontalShift + addedMarginSize
        margins.added_right_margin[node.id] = currentNameLengthHalf + currentHorizontalShift + addedMarginSize
      } else {
        let nodeMargin = Math.max(currentRadiusSize, linkNodeSize) + addedMarginSize

        margins.added_top_margin[node.id] = height - graphHeight - 2 + nodeMargin
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
    return Math.max(
      nodeMargins.added_left_margin[id] ?? addedMarginSize,
      Math.min(width - (nodeMargins.added_right_margin[id] ?? addedMarginSize), xLoc ?? graphCenterX)
    )
  }

  function getYAdjusted(id, yLoc) {
    return Math.max(
      nodeMargins.added_top_margin[id] ?? addedMarginSize,
      Math.min(height - (nodeMargins.added_bottom_margin[id] ?? addedMarginSize), yLoc ?? graphCenterY)
    )
  }

  function linkSourceId(link) {
    return typeof link.source == "object" ? link.source.id : link.source
  }

  function linkTargetId(link) {
    return typeof link.target == "object" ? link.target.id : link.target
  }

  function linkSourceX(link) {
    return getXAdjusted(linkSourceId(link), link.source?.x)
  }

  function linkSourceY(link) {
    return getYAdjusted(linkSourceId(link), link.source?.y)
  }

  function linkTargetX(link) {
    return getXAdjusted(linkTargetId(link), link.target?.x)
  }

  function linkTargetY(link) {
    return getYAdjusted(linkTargetId(link), link.target?.y)
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

  function nodeSizeWarningPosition(node) {
    let label = labelPosition(node)
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
      nodeDescriptorValue.value == "battle_deaths_z"
        ? numberValue(node.battle_deaths)
        : numberValue(node[nodeDescriptorValue.value])

    return value == null ? "Unknown" : value.toLocaleString()
  }

  function linkHasDescriptor(link) {
    return linkDescriptorValue?.value && Number(link[linkDescriptorValue.value] || 0) > 0
  }

  function applyLegacySizing() {
    ;[nodeDescriptiveValues, maxDomain, stdNullRadiusSize] = getNodeDescriptiveValues(
      nodeDescriptorValue?.value || null
    )
    nodeMargins = getNodeMargins()
  }

  function createLegacySimulation() {
    if (simulation) {
      simulation.stop()
    }

    if (!nodes.length || !width || !height) return

    let nodeById = new Map(nodes.map(node => [node.id, node]))

    linkNodes = links
      .map(link => ({
        source: nodeById.get(linkSourceId(link)),
        target: nodeById.get(linkTargetId(link)),
      }))
      .filter(linkNode => linkNode.source && linkNode.target)

    let averageNodeRadius = averageValue(Object.values(nodeMargins.radius_size))
    let averageHorizontalNameShift = averageValue(Object.values(nodeMargins.horizontal_name_shift))

    simulation = forceSimulation(nodes.concat(linkNodes))
      .force("charge", forceManyBody().strength(-1000))
      .force("center", forceCenter(graphCenterX, graphCenterY))
      .force("x", forceX(graphCenterX).strength(0.15))
      .force("y", forceY(graphHeight - addedMarginSize * 2).strength(0.5))
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
        nodes = nodes
        links = links
      })
  }

  function graphPoint(event) {
    let rect = svg.getBoundingClientRect()

    return {
      x: ((event.clientX - rect.left) / rect.width) * width,
      y: ((event.clientY - rect.top) / rect.height) * height,
    }
  }

  function tooltipPoint(event) {
    let rect = svg.parentElement.getBoundingClientRect()

    return {
      x: event.clientX - rect.left + 16,
      y: event.clientY - rect.top + 16,
    }
  }

  function showTooltip(node, event) {
    hoverNode = node
    tooltip = { node, ...tooltipPoint(event) }
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
    tooltip = null
    let point = graphPoint(event)
    node.fx = point.x
    node.fy = point.y

    if (simulation) {
      simulation.alphaTarget(0.3).restart()
    }
  }

  function drag(event) {
    if (!dragNode) return

    let point = graphPoint(event)
    dragNode.fx = point.x
    dragNode.fy = point.y
    dragNode.x = point.x
    dragNode.y = point.y
    nodes = nodes
    links = links
  }

  function endDrag() {
    if (!dragNode) return

    dragNode.fx = null
    dragNode.fy = null
    dragNode = null

    if (simulation) {
      simulation.alphaTarget(0)
    }
  }

  function resetSimulation() {
    if (simulation) {
      simulation.stop()
    }

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
    links = links
  }

  $: if (graph !== currentGraph || width !== currentWidth) {
    currentGraph = graph
    currentWidth = width
    resetSimulation()
  }

  $: availableTimeframeItems = timeframeItems.filter(
    item =>
      item.value == "z" ||
      descriptorNodes.some(node =>
        Object.keys(node).some(field => field.endsWith(`_${item.value}`) && numberValue(node[field]) != null)
      ) ||
      descriptorLinks.some(link =>
        Object.keys(link).some(field => field.endsWith(`_${item.value}`) && numberValue(link[field]) != null)
      )
  )
  $: if (!availableTimeframeItems.some(item => item.value == timeframeValue?.value)) {
    timeframeValue = availableTimeframeItems[availableTimeframeItems.length - 1] || timeframeItems[2]
  }
  $: nodeDescriptorItems = nodeFieldItems(descriptorNodes, timeframeValue?.value || "z")
  $: linkDescriptorItems = linkFieldItems(descriptorLinks, timeframeValue?.value || "z")
  $: if (!nodeDescriptorItems.some(item => item.value == nodeDescriptorValue?.value)) {
    nodeDescriptorValue = nodeDescriptorItems[0]
  }
  $: if (!linkDescriptorItems.some(item => item.value == linkDescriptorValue?.value)) {
    linkDescriptorValue = linkDescriptorItems[0]
  }
  $: sizingSignature = `${nodeDescriptorValue?.value || "none"}|${width}|${height}|${nodes.length}|${links.length}`
  $: if (nodes.length && sizingSignature != currentSizingSignature) {
    currentSizingSignature = sizingSignature
    applyLegacySizing()
    createLegacySimulation()
  }
  $: linkDescriptorSignature = linkDescriptorValue?.value || "none"
  $: if (linkDescriptorSignature != currentLinkDescriptorSignature) {
    currentLinkDescriptorSignature = linkDescriptorSignature
    triggerLinkDashPulse()
  }

  $: timeframe = selectedWar
    ? selectedWar.ongoing_conflict
      ? `${selectedWar.start_year}-Present`
      : selectedWar.start_year == selectedWar.end_year
        ? selectedWar.start_year
        : `${selectedWar.start_year}-${selectedWar.end_year}`
    : ""

  onDestroy(() => {
    if (simulation) {
      simulation.stop()
    }
    clearTimeout(linkDashPulseTimer)
  })
</script>

<svelte:window on:pointermove={drag} on:pointerup={endDrag} />

<section class="min-h-[690px] border border-[#d2d7d3] bg-[#fbfcf9]" bind:clientWidth={width}>
  <div class="flex flex-col gap-3 border-b border-[#d2d7d3] bg-white px-4 py-3">
    <div class="flex flex-col gap-1 sm:flex-row sm:items-end sm:justify-between">
      <div>
        <h2 class="text-lg font-extrabold">{selectedWar?.war_name || "Select a war"}</h2>
        <p class="text-sm text-[#60706a]">{selectedWar ? `${selectedWar.war_type} | ${timeframe}` : ""}</p>
      </div>
      {#if selectedWar}
        <div class="flex gap-3 text-sm font-bold text-[#344348]">
          <span>{selectedWar.total_participants} participants</span>
          <span>{selectedWar.total_dyads} dyads</span>
        </div>
      {/if}
    </div>

    {#if nodes.length}
      <div class="grid gap-3 md:grid-cols-3">
        <div>
          <div class="mb-1 text-xs font-extrabold uppercase tracking-[0.16em] text-[#596b64]">Timeframe</div>
          <Select items={availableTimeframeItems} bind:value={timeframeValue} clearable={false} />
        </div>
        <div>
          <div class="mb-1 text-xs font-extrabold uppercase tracking-[0.16em] text-[#596b64]">Node Size</div>
          <Select items={nodeDescriptorItems} bind:value={nodeDescriptorValue} clearable={false} />
        </div>
        <div>
          <div class="mb-1 text-xs font-extrabold uppercase tracking-[0.16em] text-[#596b64]">Link Dashes</div>
          <Select
            items={linkDescriptorItems}
            bind:value={linkDescriptorValue}
            clearable={false}
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
        on:pointerleave={() => {
          if (!dragNode) {
            hoverNode = null
            tooltip = null
          }
        }}
      >
        <rect {width} {height} fill="#fbfcf9" />
        <g>
          {#each links as link, i (i)}
            <line
              x1={linkSourceX(link)}
              y1={linkSourceY(link)}
              x2={linkTargetX(link)}
              y2={linkTargetY(link)}
              stroke="#8a948f"
              stroke-opacity="0.45"
              stroke-width="1"
            />
            <line
              x1={linkSourceX(link)}
              y1={linkSourceY(link)}
              x2={linkTargetX(link)}
              y2={linkTargetY(link)}
              stroke={linkHasDescriptor(link) ? "blue" : "transparent"}
              stroke-opacity="0.9"
              stroke-width={linkDashStrokeWidth}
              stroke-dasharray="2.5 15"
              stroke-dashoffset="-7.5"
              style="transition: stroke-width 1000ms ease 50ms, stroke 1000ms ease 50ms;"
            />
          {/each}
        </g>
        <g role="list">
          {#each nodes as node (node.id)}
            <g
              class="cursor-grab active:cursor-grabbing"
              role="listitem"
              transform="translate({getXAdjusted(node.id, node.x)}, {getYAdjusted(node.id, node.y)})"
              on:pointerdown={event => startDrag(node, event)}
              on:pointerenter={event => showTooltip(node, event)}
              on:pointermove={event => showTooltip(node, event)}
              on:pointerleave={() => {
                if (!dragNode) {
                  hoverNode = null
                  tooltip = null
                }
              }}
            >
              <circle
                r={nodeRadius(node)}
                fill={sideColors[node.side]}
                stroke="black"
                stroke-width={hoverNode?.id == node.id ? nodeStrokeWidth + 0.75 : nodeStrokeWidth}
                style="transition: r 3000ms ease 500ms, stroke-width 150ms ease;"
              />
              <text
                class="text-[12px] font-bold"
                x={labelPosition(node).x}
                y={labelPosition(node).y}
                text-anchor={labelPosition(node).anchor}
                fill={labelPosition(node).inside ? "white" : "#111827"}
                stroke={labelPosition(node).inside ? "none" : "white"}
                stroke-width={labelPosition(node).inside ? 0 : 3}
                paint-order="stroke"
                style="transition: x 2000ms ease 1500ms, y 2000ms ease 1500ms, fill 2000ms ease 1500ms, stroke 2000ms ease 1500ms;"
              >
                {node.participant}
              </text>
              {#if showNodeSizeWarning(node)}
                <text
                  class="text-[12px] font-bold"
                  x={nodeSizeWarningPosition(node).x}
                  y={nodeSizeWarningPosition(node).y}
                  text-anchor="middle"
                  fill="#111827"
                  stroke="white"
                  stroke-width="3"
                  paint-order="stroke"
                  style="transition: x 2000ms ease 1500ms, y 2000ms ease 1500ms;"
                >
                  ?
                </text>
              {/if}
            </g>
          {/each}
        </g>
      </svg>

      {#if tooltip}
        <div
          class="pointer-events-none absolute z-20 max-w-xs border border-[#c4cec8] bg-white px-3 py-2 text-sm shadow-sm"
          style="left: {tooltip.x}px; top: {tooltip.y}px;"
        >
          <div class="font-extrabold">{tooltip.node.participant}</div>
          <div class="mt-1 text-[#50615b]">Side {tooltip.node.side || "unknown"}</div>
          <div class="text-[#50615b]">Battle deaths: {Number(tooltip.node.battle_deaths || 0).toLocaleString()}</div>
          {#if nodeDescriptorValue?.value}
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
