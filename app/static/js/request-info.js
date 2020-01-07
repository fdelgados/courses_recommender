window.onload = function () {
    $(document).on("click", ".request-info-btn", function () {
        var courseId = $(this).data('courseId');

        $("#requestInfoModal").find('input[name=courseId]').val(courseId);
        $('#requestInfoModal').modal('show');
    });
}
