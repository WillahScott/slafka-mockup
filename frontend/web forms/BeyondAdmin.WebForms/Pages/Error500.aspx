<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="Error500.aspx.cs" Inherits="BeyondAdmin.WebForms.Pages.Error500" %>

<!DOCTYPE html>

<html lang="en">
<head runat="server">
    <meta charset="utf-8" />
    <meta name="description" content="@ViewBag.Description" />
    <meta http-equiv="X-UA-Compatible" content="IE=edge" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
    <link rel="shortcut icon" href="/assets/img/favicon.png" type="image/x-icon">

    <title><%: Page.Title %></title>

    <!--Basic Styles-->
    <%: Styles.Render("~/css/bootstrap") %>
    <link id="bootstrap_rtl_link" href="" rel="stylesheet" />


    <!--Fonts-->
    <link href="http://fonts.googleapis.com/css?family=Open+Sans:300italic,400italic,600italic,700italic,400,600,700,300"
        rel="stylesheet" type="text/css">

    <!--Beyond styles-->
    <%: Styles.Render("~/css/beyond") %>
    <link id="skin_link" href="" rel="stylesheet" type="text/css" />

    <!--Skin Script: Place this script in head to load scripts for skins and rtl support-->
    <%: Scripts.Render("~/bundles/skin") %>

    <link href="~/assets/img/favicon.png" rel="shortcut icon" type="image/x-icon" />

</head>
<body class="body-500">
    <form runat="server">
        <div class="error-header"></div>
        <div class="container ">
            <section class="error-container text-center">
                <h1>500</h1>
                <div class="error-divider">
                    <h2>ooops!!</h2>
                    <p class="description">SOMETHING WENT WRONG.</p>
                </div>
                <a href="/Default.aspx" class="return-btn"><i class="fa fa-home"></i>Home</a>
            </section>
        </div>
    </form>
</body>
</html>
