do ->
    $("ul li a").one("mouseover", ->
        $a = $(this)
        $.ajax(
            type: "POST"
            data: $a.attr("href").slice(1)
            dataType: "text"
            success: (data) ->
                $a.attr("title", data)
        )
    )
