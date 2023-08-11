const fs = require('fs');
const path = require('path');

module.exports.createApiKey = fs.readFileSync(path.join(__dirname, 'createApiKey.gql'), 'utf8');
module.exports.deleteApiKey = fs.readFileSync(path.join(__dirname, 'deleteApiKey.gql'), 'utf8');
module.exports.updateApiKey = fs.readFileSync(path.join(__dirname, 'updateApiKey.gql'), 'utf8');
module.exports.createProcedure = fs.readFileSync(path.join(__dirname, 'createProcedure.gql'), 'utf8');
module.exports.updateProcedure = fs.readFileSync(path.join(__dirname, 'updateProcedure.gql'), 'utf8');
module.exports.deleteProcedure = fs.readFileSync(path.join(__dirname, 'deleteProcedure.gql'), 'utf8');
module.exports.cloneProcedure = fs.readFileSync(path.join(__dirname, 'cloneProcedure.gql'), 'utf8');
module.exports.updateProcedureAttribute = fs.readFileSync(path.join(__dirname, 'updateProcedureAttribute.gql'), 'utf8');
module.exports.createProcedureVersion = fs.readFileSync(path.join(__dirname, 'createProcedureVersion.gql'), 'utf8');
module.exports.addLabelToProcedureFamily = fs.readFileSync(path.join(__dirname, 'addLabelToProcedureFamily.gql'), 'utf8');
module.exports.removeLabelFromProcedureFamily = fs.readFileSync(path.join(__dirname, 'removeLabelFromProcedureFamily.gql'), 'utf8');
module.exports.copyStandardStep = fs.readFileSync(path.join(__dirname, 'copyStandardStep.gql'), 'utf8');
module.exports.createStandardStepVersion = fs.readFileSync(path.join(__dirname, 'createStandardStepVersion.gql'), 'utf8');
module.exports.createMbomSubstitutes = fs.readFileSync(path.join(__dirname, 'createMbomSubstitutes.gql'), 'utf8');
module.exports.createMultipleMbomItems = fs.readFileSync(path.join(__dirname, 'createMultipleMbomItems.gql'), 'utf8');
module.exports.createAbomForPartInventory = fs.readFileSync(path.join(__dirname, 'createAbomForPartInventory.gql'), 'utf8');
module.exports.installKitOnAbomItemChildren = fs.readFileSync(path.join(__dirname, 'installKitOnAbomItemChildren.gql'), 'utf8');
module.exports.createStep = fs.readFileSync(path.join(__dirname, 'createStep.gql'), 'utf8');
module.exports.updateStep = fs.readFileSync(path.join(__dirname, 'updateStep.gql'), 'utf8');
module.exports.deleteStep = fs.readFileSync(path.join(__dirname, 'deleteStep.gql'), 'utf8');
module.exports.copyStep = fs.readFileSync(path.join(__dirname, 'copyStep.gql'), 'utf8');
module.exports.reorderSteps = fs.readFileSync(path.join(__dirname, 'reorderSteps.gql'), 'utf8');
module.exports.importStepsFromPdf = fs.readFileSync(path.join(__dirname, 'importStepsFromPdf.gql'), 'utf8');
module.exports.stepDatagridOperations = fs.readFileSync(path.join(__dirname, 'stepDatagridOperations.gql'), 'utf8');
module.exports.createStepField = fs.readFileSync(path.join(__dirname, 'createStepField.gql'), 'utf8');
module.exports.updateStepField = fs.readFileSync(path.join(__dirname, 'updateStepField.gql'), 'utf8');
module.exports.deleteStepField = fs.readFileSync(path.join(__dirname, 'deleteStepField.gql'), 'utf8');
module.exports.reorderStepFields = fs.readFileSync(path.join(__dirname, 'reorderStepFields.gql'), 'utf8');
module.exports.updateStepAttribute = fs.readFileSync(path.join(__dirname, 'updateStepAttribute.gql'), 'utf8');
module.exports.createRunStep = fs.readFileSync(path.join(__dirname, 'createRunStep.gql'), 'utf8');
module.exports.updateRunStep = fs.readFileSync(path.join(__dirname, 'updateRunStep.gql'), 'utf8');
module.exports.reorderRunSteps = fs.readFileSync(path.join(__dirname, 'reorderRunSteps.gql'), 'utf8');
module.exports.updateRunStepAttribute = fs.readFileSync(path.join(__dirname, 'updateRunStepAttribute.gql'), 'utf8');
module.exports.updateRunStepField = fs.readFileSync(path.join(__dirname, 'updateRunStepField.gql'), 'utf8');
module.exports.updateRunStepFieldValue = fs.readFileSync(path.join(__dirname, 'updateRunStepFieldValue.gql'), 'utf8');
module.exports.cancelRunStepRedline = fs.readFileSync(path.join(__dirname, 'cancelRunStepRedline.gql'), 'utf8');
module.exports.runStepDatagridOperations = fs.readFileSync(path.join(__dirname, 'runStepDatagridOperations.gql'), 'utf8');
module.exports.createRuns = fs.readFileSync(path.join(__dirname, 'createRuns.gql'), 'utf8');
module.exports.cancelRun = fs.readFileSync(path.join(__dirname, 'cancelRun.gql'), 'utf8');
module.exports.archiveRun = fs.readFileSync(path.join(__dirname, 'archiveRun.gql'), 'utf8');
module.exports.updateRuns = fs.readFileSync(path.join(__dirname, 'updateRuns.gql'), 'utf8');
module.exports.updateRunAttribute = fs.readFileSync(path.join(__dirname, 'updateRunAttribute.gql'), 'utf8');
module.exports.generateRunSummary = fs.readFileSync(path.join(__dirname, 'generateRunSummary.gql'), 'utf8');
module.exports.copyStepToRun = fs.readFileSync(path.join(__dirname, 'copyStepToRun.gql'), 'utf8');
module.exports.createFileAttachment = fs.readFileSync(path.join(__dirname, 'createFileAttachment.gql'), 'utf8');
module.exports.deleteFileAttachment = fs.readFileSync(path.join(__dirname, 'deleteFileAttachment.gql'), 'utf8');
module.exports.resendInvite = fs.readFileSync(path.join(__dirname, 'resendInvite.gql'), 'utf8');
module.exports.revokeInvite = fs.readFileSync(path.join(__dirname, 'revokeInvite.gql'), 'utf8');
module.exports.attachRoleToUser = fs.readFileSync(path.join(__dirname, 'attachRoleToUser.gql'), 'utf8');
module.exports.detachRoleFromUser = fs.readFileSync(path.join(__dirname, 'detachRoleFromUser.gql'), 'utf8');
module.exports.deleteRole = fs.readFileSync(path.join(__dirname, 'deleteRole.gql'), 'utf8');
module.exports.deleteTeam = fs.readFileSync(path.join(__dirname, 'deleteTeam.gql'), 'utf8');
module.exports.addUserToTeam = fs.readFileSync(path.join(__dirname, 'addUserToTeam.gql'), 'utf8');
module.exports.attachRoleToTeam = fs.readFileSync(path.join(__dirname, 'attachRoleToTeam.gql'), 'utf8');
module.exports.detachRoleFromTeam = fs.readFileSync(path.join(__dirname, 'detachRoleFromTeam.gql'), 'utf8');
module.exports.removeUserFromTeam = fs.readFileSync(path.join(__dirname, 'removeUserFromTeam.gql'), 'utf8');
module.exports.createAsset = fs.readFileSync(path.join(__dirname, 'createAsset.gql'), 'utf8');
module.exports.deleteAsset = fs.readFileSync(path.join(__dirname, 'deleteAsset.gql'), 'utf8');
module.exports.createPart = fs.readFileSync(path.join(__dirname, 'createPart.gql'), 'utf8');
module.exports.updatePart = fs.readFileSync(path.join(__dirname, 'updatePart.gql'), 'utf8');
module.exports.createOrUpdateMultipleParts = fs.readFileSync(path.join(__dirname, 'createOrUpdateMultipleParts.gql'), 'utf8');
module.exports.updatePartAttribute = fs.readFileSync(path.join(__dirname, 'updatePartAttribute.gql'), 'utf8');
module.exports.createPartRevision = fs.readFileSync(path.join(__dirname, 'createPartRevision.gql'), 'utf8');
module.exports.moveItemToInventory = fs.readFileSync(path.join(__dirname, 'moveItemToInventory.gql'), 'utf8');
module.exports.checkIn = fs.readFileSync(path.join(__dirname, 'checkIn.gql'), 'utf8');
module.exports.checkOut = fs.readFileSync(path.join(__dirname, 'checkOut.gql'), 'utf8');
module.exports.createPartInventories = fs.readFileSync(path.join(__dirname, 'createPartInventories.gql'), 'utf8');
module.exports.issueItemToKit = fs.readFileSync(path.join(__dirname, 'issueItemToKit.gql'), 'utf8');
module.exports.dispatchNotification = fs.readFileSync(path.join(__dirname, 'dispatchNotification.gql'), 'utf8');
module.exports.createUserSubscription = fs.readFileSync(path.join(__dirname, 'createUserSubscription.gql'), 'utf8');
module.exports.createReviewRequest = fs.readFileSync(path.join(__dirname, 'createReviewRequest.gql'), 'utf8');
module.exports.copyField = fs.readFileSync(path.join(__dirname, 'copyField.gql'), 'utf8');
module.exports.mergeRunStepToProcedure = fs.readFileSync(path.join(__dirname, 'mergeRunStepToProcedure.gql'), 'utf8');
module.exports.mergeRunStepToRuns = fs.readFileSync(path.join(__dirname, 'mergeRunStepToRuns.gql'), 'utf8');
module.exports.addInventoryToPurchaseOrderLine = fs.readFileSync(path.join(__dirname, 'addInventoryToPurchaseOrderLine.gql'), 'utf8');
module.exports.removeInventoryFromPurchaseOrderLine = fs.readFileSync(path.join(__dirname, 'removeInventoryFromPurchaseOrderLine.gql'), 'utf8');
module.exports.addLabelToItem = fs.readFileSync(path.join(__dirname, 'addLabelToItem.gql'), 'utf8');
module.exports.removeLabelFromItem = fs.readFileSync(path.join(__dirname, 'removeLabelFromItem.gql'), 'utf8');
module.exports.reorderRunStepFields = fs.readFileSync(path.join(__dirname, 'reorderRunStepFields.gql'), 'utf8');
module.exports.addPlanItemToPlan = fs.readFileSync(path.join(__dirname, 'addPlanItemToPlan.gql'), 'utf8');
module.exports.removePlanItemFromPlan = fs.readFileSync(path.join(__dirname, 'removePlanItemFromPlan.gql'), 'utf8');
module.exports.addResultToPlanItem = fs.readFileSync(path.join(__dirname, 'addResultToPlanItem.gql'), 'utf8');
module.exports.removeResultFromPlanItem = fs.readFileSync(path.join(__dirname, 'removeResultFromPlanItem.gql'), 'utf8');
module.exports.convertResultsFromPlanItem = fs.readFileSync(path.join(__dirname, 'convertResultsFromPlanItem.gql'), 'utf8');
module.exports.addHeaderToWebhookReceiver = fs.readFileSync(path.join(__dirname, 'addHeaderToWebhookReceiver.gql'), 'utf8');
module.exports.removeHeaderFromWebhookReceiver = fs.readFileSync(path.join(__dirname, 'removeHeaderFromWebhookReceiver.gql'), 'utf8');
module.exports.addItemsToReceipt = fs.readFileSync(path.join(__dirname, 'addItemsToReceipt.gql'), 'utf8');
module.exports.removeItemFromReceipt = fs.readFileSync(path.join(__dirname, 'removeItemFromReceipt.gql'), 'utf8');
module.exports.reorderPurchaseOrderLine = fs.readFileSync(path.join(__dirname, 'reorderPurchaseOrderLine.gql'), 'utf8');
module.exports.reorderDatagridRow = fs.readFileSync(path.join(__dirname, 'reorderDatagridRow.gql'), 'utf8');
module.exports.reorderDatagridColumn = fs.readFileSync(path.join(__dirname, 'reorderDatagridColumn.gql'), 'utf8');
module.exports.setDatagridValue = fs.readFileSync(path.join(__dirname, 'setDatagridValue.gql'), 'utf8');
module.exports.addSubtypeToPart = fs.readFileSync(path.join(__dirname, 'addSubtypeToPart.gql'), 'utf8');
module.exports.removeSubtypeFromPart = fs.readFileSync(path.join(__dirname, 'removeSubtypeFromPart.gql'), 'utf8');
module.exports.updateIssueAttribute = fs.readFileSync(path.join(__dirname, 'updateIssueAttribute.gql'), 'utf8');
module.exports.updateLocationAttribute = fs.readFileSync(path.join(__dirname, 'updateLocationAttribute.gql'), 'utf8');
module.exports.updateMbomAttribute = fs.readFileSync(path.join(__dirname, 'updateMbomAttribute.gql'), 'utf8');
module.exports.updatePartInventoryAttribute = fs.readFileSync(path.join(__dirname, 'updatePartInventoryAttribute.gql'), 'utf8');
module.exports.updatePurchaseOrderAttribute = fs.readFileSync(path.join(__dirname, 'updatePurchaseOrderAttribute.gql'), 'utf8');
module.exports.updatePurchaseOrderLineAttribute = fs.readFileSync(path.join(__dirname, 'updatePurchaseOrderLineAttribute.gql'), 'utf8');
module.exports.updateSupplierAttribute = fs.readFileSync(path.join(__dirname, 'updateSupplierAttribute.gql'), 'utf8');
module.exports.updateReceiptAttribute = fs.readFileSync(path.join(__dirname, 'updateReceiptAttribute.gql'), 'utf8');
module.exports.addRequirementToItem = fs.readFileSync(path.join(__dirname, 'addRequirementToItem.gql'), 'utf8');
module.exports.removeRequirementFromItem = fs.readFileSync(path.join(__dirname, 'removeRequirementFromItem.gql'), 'utf8');
module.exports.createKitForRun = fs.readFileSync(path.join(__dirname, 'createKitForRun.gql'), 'utf8');
module.exports.splitUnfulfilledPartKit = fs.readFileSync(path.join(__dirname, 'splitUnfulfilledPartKit.gql'), 'utf8');
module.exports.updatePartKitAttribute = fs.readFileSync(path.join(__dirname, 'updatePartKitAttribute.gql'), 'utf8');
module.exports.updatePartKitItemAttribute = fs.readFileSync(path.join(__dirname, 'updatePartKitItemAttribute.gql'), 'utf8');
module.exports.moveKitInventoryToLocation = fs.readFileSync(path.join(__dirname, 'moveKitInventoryToLocation.gql'), 'utf8');
module.exports.addPartsToKitFromMbom = fs.readFileSync(path.join(__dirname, 'addPartsToKitFromMbom.gql'), 'utf8');
module.exports.addPartInventoryToRunStep = fs.readFileSync(path.join(__dirname, 'addPartInventoryToRunStep.gql'), 'utf8');
module.exports.removePartInventoryFromRunStep = fs.readFileSync(path.join(__dirname, 'removePartInventoryFromRunStep.gql'), 'utf8');
module.exports.updatePartInventory = fs.readFileSync(path.join(__dirname, 'updatePartInventory.gql'), 'utf8');
module.exports.splitPartInventory = fs.readFileSync(path.join(__dirname, 'splitPartInventory.gql'), 'utf8');
module.exports.mergePartInventory = fs.readFileSync(path.join(__dirname, 'mergePartInventory.gql'), 'utf8');
module.exports.splitManyPartInventory = fs.readFileSync(path.join(__dirname, 'splitManyPartInventory.gql'), 'utf8');
module.exports.convertResultsFromPlanItems = fs.readFileSync(path.join(__dirname, 'convertResultsFromPlanItems.gql'), 'utf8');
module.exports.updateOrganization = fs.readFileSync(path.join(__dirname, 'updateOrganization.gql'), 'utf8');
module.exports.createOrganizationPartRevisionScheme = fs.readFileSync(path.join(__dirname, 'createOrganizationPartRevisionScheme.gql'), 'utf8');
module.exports.updateOrganizationPartRevisionScheme = fs.readFileSync(path.join(__dirname, 'updateOrganizationPartRevisionScheme.gql'), 'utf8');
module.exports.deleteOrganizationPartRevisionScheme = fs.readFileSync(path.join(__dirname, 'deleteOrganizationPartRevisionScheme.gql'), 'utf8');
module.exports.createOrganizationGlobalUniqueSerialNumberScheme = fs.readFileSync(path.join(__dirname, 'createOrganizationGlobalUniqueSerialNumberScheme.gql'), 'utf8');
module.exports.updateOrganizationGlobalUniqueSerialNumberScheme = fs.readFileSync(path.join(__dirname, 'updateOrganizationGlobalUniqueSerialNumberScheme.gql'), 'utf8');
module.exports.deleteOrganizationGlobalUniqueSerialNumberScheme = fs.readFileSync(path.join(__dirname, 'deleteOrganizationGlobalUniqueSerialNumberScheme.gql'), 'utf8');
module.exports.updateOrganizationPartAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationPartAttributes.gql'), 'utf8');
module.exports.deleteOrganizationPartAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationPartAttributes.gql'), 'utf8');
module.exports.updateOrganizationPartInventoryAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationPartInventoryAttributes.gql'), 'utf8');
module.exports.deleteOrganizationPartInventoryAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationPartInventoryAttributes.gql'), 'utf8');
module.exports.updateOrganizationLocationAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationLocationAttributes.gql'), 'utf8');
module.exports.deleteOrganizationLocationAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationLocationAttributes.gql'), 'utf8');
module.exports.updateOrganizationMbomAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationMbomAttributes.gql'), 'utf8');
module.exports.deleteOrganizationMbomAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationMbomAttributes.gql'), 'utf8');
module.exports.updateOrganizationProcedureAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationProcedureAttributes.gql'), 'utf8');
module.exports.deleteOrganizationProcedureAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationProcedureAttributes.gql'), 'utf8');
module.exports.updateOrganizationPurchaseOrderAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationPurchaseOrderAttributes.gql'), 'utf8');
module.exports.deleteOrganizationPurchaseOrderAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationPurchaseOrderAttributes.gql'), 'utf8');
module.exports.updateOrganizationSupplierAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationSupplierAttributes.gql'), 'utf8');
module.exports.deleteOrganizationSupplierAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationSupplierAttributes.gql'), 'utf8');
module.exports.updateOrganizationIssueAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationIssueAttributes.gql'), 'utf8');
module.exports.deleteOrganizationIssueAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationIssueAttributes.gql'), 'utf8');
module.exports.updateOrganizationPurchaseOrderLineAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationPurchaseOrderLineAttributes.gql'), 'utf8');
module.exports.deleteOrganizationPurchaseOrderLineAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationPurchaseOrderLineAttributes.gql'), 'utf8');
module.exports.updateOrganizationPartKitAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationPartKitAttributes.gql'), 'utf8');
module.exports.deleteOrganizationPartKitAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationPartKitAttributes.gql'), 'utf8');
module.exports.updateOrganizationPartKitItemAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationPartKitItemAttributes.gql'), 'utf8');
module.exports.deleteOrganizationPartKitItemAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationPartKitItemAttributes.gql'), 'utf8');
module.exports.updateOrganizationReceiptAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationReceiptAttributes.gql'), 'utf8');
module.exports.deleteOrganizationReceiptAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationReceiptAttributes.gql'), 'utf8');
module.exports.updateOrganizationRunAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationRunAttributes.gql'), 'utf8');
module.exports.deleteOrganizationRunAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationRunAttributes.gql'), 'utf8');
module.exports.attachPermissionGroupToRole = fs.readFileSync(path.join(__dirname, 'attachPermissionGroupToRole.gql'), 'utf8');
module.exports.detachPermissionGroupFromRole = fs.readFileSync(path.join(__dirname, 'detachPermissionGroupFromRole.gql'), 'utf8');
module.exports.updateOrganizationRunStepAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationRunStepAttributes.gql'), 'utf8');
module.exports.deleteOrganizationRunStepAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationRunStepAttributes.gql'), 'utf8');
module.exports.updateOrganizationStepAttributes = fs.readFileSync(path.join(__dirname, 'updateOrganizationStepAttributes.gql'), 'utf8');
module.exports.deleteOrganizationStepAttributes = fs.readFileSync(path.join(__dirname, 'deleteOrganizationStepAttributes.gql'), 'utf8');
module.exports.deletePurchaseOrder = fs.readFileSync(path.join(__dirname, 'deletePurchaseOrder.gql'), 'utf8');
module.exports.deleteIssueDispositionTypeRole = fs.readFileSync(path.join(__dirname, 'deleteIssueDispositionTypeRole.gql'), 'utf8');
module.exports.resetIssueApprovals = fs.readFileSync(path.join(__dirname, 'resetIssueApprovals.gql'), 'utf8');
module.exports.generateReadEmbeddedAnalytics = fs.readFileSync(path.join(__dirname, 'generateReadEmbeddedAnalytics.gql'), 'utf8');
module.exports.generateWriteEmbeddedAnalytics = fs.readFileSync(path.join(__dirname, 'generateWriteEmbeddedAnalytics.gql'), 'utf8');
module.exports.deleteMbomApprovalRole = fs.readFileSync(path.join(__dirname, 'deleteMbomApprovalRole.gql'), 'utf8');
module.exports.deleteIssuePartInventory = fs.readFileSync(path.join(__dirname, 'deleteIssuePartInventory.gql'), 'utf8');
module.exports.updateAbomItem = fs.readFileSync(path.join(__dirname, 'updateAbomItem.gql'), 'utf8');
module.exports.createAbomItem = fs.readFileSync(path.join(__dirname, 'createAbomItem.gql'), 'utf8');
module.exports.deleteAbomItem = fs.readFileSync(path.join(__dirname, 'deleteAbomItem.gql'), 'utf8');
module.exports.updateBarcodePattern = fs.readFileSync(path.join(__dirname, 'updateBarcodePattern.gql'), 'utf8');
module.exports.createBarcodePattern = fs.readFileSync(path.join(__dirname, 'createBarcodePattern.gql'), 'utf8');
module.exports.deleteBarcodePattern = fs.readFileSync(path.join(__dirname, 'deleteBarcodePattern.gql'), 'utf8');
module.exports.updateBarcodeLabel = fs.readFileSync(path.join(__dirname, 'updateBarcodeLabel.gql'), 'utf8');
module.exports.createBarcodeLabel = fs.readFileSync(path.join(__dirname, 'createBarcodeLabel.gql'), 'utf8');
module.exports.updateBarcodeTemplate = fs.readFileSync(path.join(__dirname, 'updateBarcodeTemplate.gql'), 'utf8');
module.exports.createBarcodeTemplate = fs.readFileSync(path.join(__dirname, 'createBarcodeTemplate.gql'), 'utf8');
module.exports.updateComment = fs.readFileSync(path.join(__dirname, 'updateComment.gql'), 'utf8');
module.exports.createComment = fs.readFileSync(path.join(__dirname, 'createComment.gql'), 'utf8');
module.exports.deleteComment = fs.readFileSync(path.join(__dirname, 'deleteComment.gql'), 'utf8');
module.exports.updateContact = fs.readFileSync(path.join(__dirname, 'updateContact.gql'), 'utf8');
module.exports.createContact = fs.readFileSync(path.join(__dirname, 'createContact.gql'), 'utf8');
module.exports.deleteContact = fs.readFileSync(path.join(__dirname, 'deleteContact.gql'), 'utf8');
module.exports.updateCurrency = fs.readFileSync(path.join(__dirname, 'updateCurrency.gql'), 'utf8');
module.exports.createCurrency = fs.readFileSync(path.join(__dirname, 'createCurrency.gql'), 'utf8');
module.exports.deleteCurrency = fs.readFileSync(path.join(__dirname, 'deleteCurrency.gql'), 'utf8');
module.exports.updateDatagridColumn = fs.readFileSync(path.join(__dirname, 'updateDatagridColumn.gql'), 'utf8');
module.exports.createDatagridColumn = fs.readFileSync(path.join(__dirname, 'createDatagridColumn.gql'), 'utf8');
module.exports.deleteDatagridColumn = fs.readFileSync(path.join(__dirname, 'deleteDatagridColumn.gql'), 'utf8');
module.exports.updateDatagridRow = fs.readFileSync(path.join(__dirname, 'updateDatagridRow.gql'), 'utf8');
module.exports.createDatagridRow = fs.readFileSync(path.join(__dirname, 'createDatagridRow.gql'), 'utf8');
module.exports.deleteDatagridRow = fs.readFileSync(path.join(__dirname, 'deleteDatagridRow.gql'), 'utf8');
module.exports.updateIntegration = fs.readFileSync(path.join(__dirname, 'updateIntegration.gql'), 'utf8');
module.exports.createIntegration = fs.readFileSync(path.join(__dirname, 'createIntegration.gql'), 'utf8');
module.exports.deleteIntegration = fs.readFileSync(path.join(__dirname, 'deleteIntegration.gql'), 'utf8');
module.exports.createInvite = fs.readFileSync(path.join(__dirname, 'createInvite.gql'), 'utf8');
module.exports.updateIssue = fs.readFileSync(path.join(__dirname, 'updateIssue.gql'), 'utf8');
module.exports.createIssue = fs.readFileSync(path.join(__dirname, 'createIssue.gql'), 'utf8');
module.exports.updateIssueApproval = fs.readFileSync(path.join(__dirname, 'updateIssueApproval.gql'), 'utf8');
module.exports.createIssueApproval = fs.readFileSync(path.join(__dirname, 'createIssueApproval.gql'), 'utf8');
module.exports.updateIssueApprovalRequest = fs.readFileSync(path.join(__dirname, 'updateIssueApprovalRequest.gql'), 'utf8');
module.exports.createIssueApprovalRequest = fs.readFileSync(path.join(__dirname, 'createIssueApprovalRequest.gql'), 'utf8');
module.exports.deleteIssueApprovalRequest = fs.readFileSync(path.join(__dirname, 'deleteIssueApprovalRequest.gql'), 'utf8');
module.exports.createIssueRelation = fs.readFileSync(path.join(__dirname, 'createIssueRelation.gql'), 'utf8');
module.exports.deleteIssueRelation = fs.readFileSync(path.join(__dirname, 'deleteIssueRelation.gql'), 'utf8');
module.exports.updateIssueDispositionType = fs.readFileSync(path.join(__dirname, 'updateIssueDispositionType.gql'), 'utf8');
module.exports.createIssueDispositionType = fs.readFileSync(path.join(__dirname, 'createIssueDispositionType.gql'), 'utf8');
module.exports.deleteIssueDispositionType = fs.readFileSync(path.join(__dirname, 'deleteIssueDispositionType.gql'), 'utf8');
module.exports.updateIssueDispositionTypeRole = fs.readFileSync(path.join(__dirname, 'updateIssueDispositionTypeRole.gql'), 'utf8');
module.exports.createIssueDispositionTypeRole = fs.readFileSync(path.join(__dirname, 'createIssueDispositionTypeRole.gql'), 'utf8');
module.exports.createIssuePartInventory = fs.readFileSync(path.join(__dirname, 'createIssuePartInventory.gql'), 'utf8');
module.exports.updateLabel = fs.readFileSync(path.join(__dirname, 'updateLabel.gql'), 'utf8');
module.exports.createLabel = fs.readFileSync(path.join(__dirname, 'createLabel.gql'), 'utf8');
module.exports.deleteLabel = fs.readFileSync(path.join(__dirname, 'deleteLabel.gql'), 'utf8');
module.exports.updateLocation = fs.readFileSync(path.join(__dirname, 'updateLocation.gql'), 'utf8');
module.exports.createLocation = fs.readFileSync(path.join(__dirname, 'createLocation.gql'), 'utf8');
module.exports.deleteLocation = fs.readFileSync(path.join(__dirname, 'deleteLocation.gql'), 'utf8');
module.exports.updateLocationSubtype = fs.readFileSync(path.join(__dirname, 'updateLocationSubtype.gql'), 'utf8');
module.exports.createLocationSubtype = fs.readFileSync(path.join(__dirname, 'createLocationSubtype.gql'), 'utf8');
module.exports.deleteLocationSubtype = fs.readFileSync(path.join(__dirname, 'deleteLocationSubtype.gql'), 'utf8');
module.exports.updateMbom = fs.readFileSync(path.join(__dirname, 'updateMbom.gql'), 'utf8');
module.exports.createMbom = fs.readFileSync(path.join(__dirname, 'createMbom.gql'), 'utf8');
module.exports.deleteMbom = fs.readFileSync(path.join(__dirname, 'deleteMbom.gql'), 'utf8');
module.exports.updateMbomItem = fs.readFileSync(path.join(__dirname, 'updateMbomItem.gql'), 'utf8');
module.exports.createMbomItem = fs.readFileSync(path.join(__dirname, 'createMbomItem.gql'), 'utf8');
module.exports.deleteMbomItem = fs.readFileSync(path.join(__dirname, 'deleteMbomItem.gql'), 'utf8');
module.exports.createMbomSubstitute = fs.readFileSync(path.join(__dirname, 'createMbomSubstitute.gql'), 'utf8');
module.exports.deleteMbomSubstitute = fs.readFileSync(path.join(__dirname, 'deleteMbomSubstitute.gql'), 'utf8');
module.exports.updateMbomApproval = fs.readFileSync(path.join(__dirname, 'updateMbomApproval.gql'), 'utf8');
module.exports.createMbomApproval = fs.readFileSync(path.join(__dirname, 'createMbomApproval.gql'), 'utf8');
module.exports.updateMbomApprovalRequest = fs.readFileSync(path.join(__dirname, 'updateMbomApprovalRequest.gql'), 'utf8');
module.exports.createMbomApprovalRequest = fs.readFileSync(path.join(__dirname, 'createMbomApprovalRequest.gql'), 'utf8');
module.exports.deleteMbomApprovalRequest = fs.readFileSync(path.join(__dirname, 'deleteMbomApprovalRequest.gql'), 'utf8');
module.exports.updateMbomApprovalRole = fs.readFileSync(path.join(__dirname, 'updateMbomApprovalRole.gql'), 'utf8');
module.exports.createMbomApprovalRole = fs.readFileSync(path.join(__dirname, 'createMbomApprovalRole.gql'), 'utf8');
module.exports.updateMbomItemReferenceDesignator = fs.readFileSync(path.join(__dirname, 'updateMbomItemReferenceDesignator.gql'), 'utf8');
module.exports.createMbomItemReferenceDesignator = fs.readFileSync(path.join(__dirname, 'createMbomItemReferenceDesignator.gql'), 'utf8');
module.exports.deleteMbomItemReferenceDesignator = fs.readFileSync(path.join(__dirname, 'deleteMbomItemReferenceDesignator.gql'), 'utf8');
module.exports.updateMrpJob = fs.readFileSync(path.join(__dirname, 'updateMrpJob.gql'), 'utf8');
module.exports.createMrpJob = fs.readFileSync(path.join(__dirname, 'createMrpJob.gql'), 'utf8');
module.exports.deletePart = fs.readFileSync(path.join(__dirname, 'deletePart.gql'), 'utf8');
module.exports.createPartInventory = fs.readFileSync(path.join(__dirname, 'createPartInventory.gql'), 'utf8');
module.exports.deletePartInventory = fs.readFileSync(path.join(__dirname, 'deletePartInventory.gql'), 'utf8');
module.exports.updatePartKit = fs.readFileSync(path.join(__dirname, 'updatePartKit.gql'), 'utf8');
module.exports.createPartKit = fs.readFileSync(path.join(__dirname, 'createPartKit.gql'), 'utf8');
module.exports.deletePartKit = fs.readFileSync(path.join(__dirname, 'deletePartKit.gql'), 'utf8');
module.exports.updatePartKitItem = fs.readFileSync(path.join(__dirname, 'updatePartKitItem.gql'), 'utf8');
module.exports.createPartKitItem = fs.readFileSync(path.join(__dirname, 'createPartKitItem.gql'), 'utf8');
module.exports.deletePartKitItem = fs.readFileSync(path.join(__dirname, 'deletePartKitItem.gql'), 'utf8');
module.exports.updatePartProcedure = fs.readFileSync(path.join(__dirname, 'updatePartProcedure.gql'), 'utf8');
module.exports.createPartProcedure = fs.readFileSync(path.join(__dirname, 'createPartProcedure.gql'), 'utf8');
module.exports.deletePartProcedure = fs.readFileSync(path.join(__dirname, 'deletePartProcedure.gql'), 'utf8');
module.exports.updatePartSubtype = fs.readFileSync(path.join(__dirname, 'updatePartSubtype.gql'), 'utf8');
module.exports.createPartSubtype = fs.readFileSync(path.join(__dirname, 'createPartSubtype.gql'), 'utf8');
module.exports.deletePartSubtype = fs.readFileSync(path.join(__dirname, 'deletePartSubtype.gql'), 'utf8');
module.exports.updatePlan = fs.readFileSync(path.join(__dirname, 'updatePlan.gql'), 'utf8');
module.exports.createPlan = fs.readFileSync(path.join(__dirname, 'createPlan.gql'), 'utf8');
module.exports.deletePlan = fs.readFileSync(path.join(__dirname, 'deletePlan.gql'), 'utf8');
module.exports.updateInputToPlan = fs.readFileSync(path.join(__dirname, 'updateInputToPlan.gql'), 'utf8');
module.exports.addInputToPlan = fs.readFileSync(path.join(__dirname, 'addInputToPlan.gql'), 'utf8');
module.exports.removeInputFromPlan = fs.readFileSync(path.join(__dirname, 'removeInputFromPlan.gql'), 'utf8');
module.exports.updatePlanItem = fs.readFileSync(path.join(__dirname, 'updatePlanItem.gql'), 'utf8');
module.exports.createPlanItem = fs.readFileSync(path.join(__dirname, 'createPlanItem.gql'), 'utf8');
module.exports.deletePlanItem = fs.readFileSync(path.join(__dirname, 'deletePlanItem.gql'), 'utf8');
module.exports.updatePlanItemAllocation = fs.readFileSync(path.join(__dirname, 'updatePlanItemAllocation.gql'), 'utf8');
module.exports.createPlanItemAllocation = fs.readFileSync(path.join(__dirname, 'createPlanItemAllocation.gql'), 'utf8');
module.exports.deletePlanItemAllocation = fs.readFileSync(path.join(__dirname, 'deletePlanItemAllocation.gql'), 'utf8');
module.exports.updatePlanReservation = fs.readFileSync(path.join(__dirname, 'updatePlanReservation.gql'), 'utf8');
module.exports.createPlanReservation = fs.readFileSync(path.join(__dirname, 'createPlanReservation.gql'), 'utf8');
module.exports.deletePlanReservation = fs.readFileSync(path.join(__dirname, 'deletePlanReservation.gql'), 'utf8');
module.exports.updatePurchaseOrder = fs.readFileSync(path.join(__dirname, 'updatePurchaseOrder.gql'), 'utf8');
module.exports.createPurchaseOrder = fs.readFileSync(path.join(__dirname, 'createPurchaseOrder.gql'), 'utf8');
module.exports.updatePurchaseOrderApproval = fs.readFileSync(path.join(__dirname, 'updatePurchaseOrderApproval.gql'), 'utf8');
module.exports.createPurchaseOrderApproval = fs.readFileSync(path.join(__dirname, 'createPurchaseOrderApproval.gql'), 'utf8');
module.exports.updatePurchaseOrderApprovalRequest = fs.readFileSync(path.join(__dirname, 'updatePurchaseOrderApprovalRequest.gql'), 'utf8');
module.exports.createPurchaseOrderApprovalRequest = fs.readFileSync(path.join(__dirname, 'createPurchaseOrderApprovalRequest.gql'), 'utf8');
module.exports.deletePurchaseOrderApprovalRequest = fs.readFileSync(path.join(__dirname, 'deletePurchaseOrderApprovalRequest.gql'), 'utf8');
module.exports.updatePurchaseOrderFee = fs.readFileSync(path.join(__dirname, 'updatePurchaseOrderFee.gql'), 'utf8');
module.exports.createPurchaseOrderFee = fs.readFileSync(path.join(__dirname, 'createPurchaseOrderFee.gql'), 'utf8');
module.exports.deletePurchaseOrderFee = fs.readFileSync(path.join(__dirname, 'deletePurchaseOrderFee.gql'), 'utf8');
module.exports.updatePurchaseOrderLine = fs.readFileSync(path.join(__dirname, 'updatePurchaseOrderLine.gql'), 'utf8');
module.exports.createPurchaseOrderLine = fs.readFileSync(path.join(__dirname, 'createPurchaseOrderLine.gql'), 'utf8');
module.exports.deletePurchaseOrderLine = fs.readFileSync(path.join(__dirname, 'deletePurchaseOrderLine.gql'), 'utf8');
module.exports.updateReceipt = fs.readFileSync(path.join(__dirname, 'updateReceipt.gql'), 'utf8');
module.exports.createReceipt = fs.readFileSync(path.join(__dirname, 'createReceipt.gql'), 'utf8');
module.exports.deleteReceipt = fs.readFileSync(path.join(__dirname, 'deleteReceipt.gql'), 'utf8');
module.exports.updateReceiptItem = fs.readFileSync(path.join(__dirname, 'updateReceiptItem.gql'), 'utf8');
module.exports.updateRedline = fs.readFileSync(path.join(__dirname, 'updateRedline.gql'), 'utf8');
module.exports.updateRedlineApproval = fs.readFileSync(path.join(__dirname, 'updateRedlineApproval.gql'), 'utf8');
module.exports.createRedlineApproval = fs.readFileSync(path.join(__dirname, 'createRedlineApproval.gql'), 'utf8');
module.exports.updateRedlineApprovalRequest = fs.readFileSync(path.join(__dirname, 'updateRedlineApprovalRequest.gql'), 'utf8');
module.exports.createRedlineApprovalRequest = fs.readFileSync(path.join(__dirname, 'createRedlineApprovalRequest.gql'), 'utf8');
module.exports.deleteRedlineApprovalRequest = fs.readFileSync(path.join(__dirname, 'deleteRedlineApprovalRequest.gql'), 'utf8');
module.exports.updateRequirement = fs.readFileSync(path.join(__dirname, 'updateRequirement.gql'), 'utf8');
module.exports.createRequirement = fs.readFileSync(path.join(__dirname, 'createRequirement.gql'), 'utf8');
module.exports.deleteRequirement = fs.readFileSync(path.join(__dirname, 'deleteRequirement.gql'), 'utf8');
module.exports.updateReview = fs.readFileSync(path.join(__dirname, 'updateReview.gql'), 'utf8');
module.exports.createReview = fs.readFileSync(path.join(__dirname, 'createReview.gql'), 'utf8');
module.exports.updateReviewRequest = fs.readFileSync(path.join(__dirname, 'updateReviewRequest.gql'), 'utf8');
module.exports.deleteReviewRequest = fs.readFileSync(path.join(__dirname, 'deleteReviewRequest.gql'), 'utf8');
module.exports.updateRole = fs.readFileSync(path.join(__dirname, 'updateRole.gql'), 'utf8');
module.exports.createRole = fs.readFileSync(path.join(__dirname, 'createRole.gql'), 'utf8');
module.exports.updateRule = fs.readFileSync(path.join(__dirname, 'updateRule.gql'), 'utf8');
module.exports.createRule = fs.readFileSync(path.join(__dirname, 'createRule.gql'), 'utf8');
module.exports.deleteRule = fs.readFileSync(path.join(__dirname, 'deleteRule.gql'), 'utf8');
module.exports.updateRun = fs.readFileSync(path.join(__dirname, 'updateRun.gql'), 'utf8');
module.exports.createRun = fs.readFileSync(path.join(__dirname, 'createRun.gql'), 'utf8');
module.exports.updateRunBatch = fs.readFileSync(path.join(__dirname, 'updateRunBatch.gql'), 'utf8');
module.exports.createRunBatch = fs.readFileSync(path.join(__dirname, 'createRunBatch.gql'), 'utf8');
module.exports.deleteRunStep = fs.readFileSync(path.join(__dirname, 'deleteRunStep.gql'), 'utf8');
module.exports.createRunStepField = fs.readFileSync(path.join(__dirname, 'createRunStepField.gql'), 'utf8');
module.exports.deleteRunStepField = fs.readFileSync(path.join(__dirname, 'deleteRunStepField.gql'), 'utf8');
module.exports.updateRunStepFieldValidation = fs.readFileSync(path.join(__dirname, 'updateRunStepFieldValidation.gql'), 'utf8');
module.exports.createRunStepFieldValidation = fs.readFileSync(path.join(__dirname, 'createRunStepFieldValidation.gql'), 'utf8');
module.exports.deleteRunStepFieldValidation = fs.readFileSync(path.join(__dirname, 'deleteRunStepFieldValidation.gql'), 'utf8');
module.exports.createRunStepEdge = fs.readFileSync(path.join(__dirname, 'createRunStepEdge.gql'), 'utf8');
module.exports.deleteRunStepEdge = fs.readFileSync(path.join(__dirname, 'deleteRunStepEdge.gql'), 'utf8');
module.exports.updateStepApproval = fs.readFileSync(path.join(__dirname, 'updateStepApproval.gql'), 'utf8');
module.exports.createStepApproval = fs.readFileSync(path.join(__dirname, 'createStepApproval.gql'), 'utf8');
module.exports.updateStepApprovalRequest = fs.readFileSync(path.join(__dirname, 'updateStepApprovalRequest.gql'), 'utf8');
module.exports.createStepApprovalRequest = fs.readFileSync(path.join(__dirname, 'createStepApprovalRequest.gql'), 'utf8');
module.exports.deleteStepApprovalRequest = fs.readFileSync(path.join(__dirname, 'deleteStepApprovalRequest.gql'), 'utf8');
module.exports.updateStepFieldValidation = fs.readFileSync(path.join(__dirname, 'updateStepFieldValidation.gql'), 'utf8');
module.exports.createStepFieldValidation = fs.readFileSync(path.join(__dirname, 'createStepFieldValidation.gql'), 'utf8');
module.exports.deleteStepFieldValidation = fs.readFileSync(path.join(__dirname, 'deleteStepFieldValidation.gql'), 'utf8');
module.exports.createStepEdge = fs.readFileSync(path.join(__dirname, 'createStepEdge.gql'), 'utf8');
module.exports.deleteStepEdge = fs.readFileSync(path.join(__dirname, 'deleteStepEdge.gql'), 'utf8');
module.exports.updateStepMbomItemAssociation = fs.readFileSync(path.join(__dirname, 'updateStepMbomItemAssociation.gql'), 'utf8');
module.exports.createStepMbomItemAssociation = fs.readFileSync(path.join(__dirname, 'createStepMbomItemAssociation.gql'), 'utf8');
module.exports.deleteStepMbomItemAssociation = fs.readFileSync(path.join(__dirname, 'deleteStepMbomItemAssociation.gql'), 'utf8');
module.exports.updateSupplier = fs.readFileSync(path.join(__dirname, 'updateSupplier.gql'), 'utf8');
module.exports.createSupplier = fs.readFileSync(path.join(__dirname, 'createSupplier.gql'), 'utf8');
module.exports.deleteSupplier = fs.readFileSync(path.join(__dirname, 'deleteSupplier.gql'), 'utf8');
module.exports.updateTeam = fs.readFileSync(path.join(__dirname, 'updateTeam.gql'), 'utf8');
module.exports.createTeam = fs.readFileSync(path.join(__dirname, 'createTeam.gql'), 'utf8');
module.exports.updateUnitOfMeasurement = fs.readFileSync(path.join(__dirname, 'updateUnitOfMeasurement.gql'), 'utf8');
module.exports.createUnitOfMeasurement = fs.readFileSync(path.join(__dirname, 'createUnitOfMeasurement.gql'), 'utf8');
module.exports.deleteUnitOfMeasurement = fs.readFileSync(path.join(__dirname, 'deleteUnitOfMeasurement.gql'), 'utf8');
module.exports.updateUser = fs.readFileSync(path.join(__dirname, 'updateUser.gql'), 'utf8');
module.exports.updateUserNotification = fs.readFileSync(path.join(__dirname, 'updateUserNotification.gql'), 'utf8');
module.exports.deleteUserSubscription = fs.readFileSync(path.join(__dirname, 'deleteUserSubscription.gql'), 'utf8');
module.exports.updateWebhookHeader = fs.readFileSync(path.join(__dirname, 'updateWebhookHeader.gql'), 'utf8');
module.exports.createWebhookHeader = fs.readFileSync(path.join(__dirname, 'createWebhookHeader.gql'), 'utf8');
module.exports.deleteWebhookHeader = fs.readFileSync(path.join(__dirname, 'deleteWebhookHeader.gql'), 'utf8');
module.exports.updateWebhookReceiver = fs.readFileSync(path.join(__dirname, 'updateWebhookReceiver.gql'), 'utf8');
module.exports.createWebhookReceiver = fs.readFileSync(path.join(__dirname, 'createWebhookReceiver.gql'), 'utf8');
module.exports.deleteWebhookReceiver = fs.readFileSync(path.join(__dirname, 'deleteWebhookReceiver.gql'), 'utf8');
module.exports.updateWebhookSubscription = fs.readFileSync(path.join(__dirname, 'updateWebhookSubscription.gql'), 'utf8');
module.exports.createWebhookSubscription = fs.readFileSync(path.join(__dirname, 'createWebhookSubscription.gql'), 'utf8');
module.exports.deleteWebhookSubscription = fs.readFileSync(path.join(__dirname, 'deleteWebhookSubscription.gql'), 'utf8');
